/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.dataagent.workflow.node;

import com.alibaba.cloud.ai.dataagent.dto.schema.SchemaDTO;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.dto.planner.Plan;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.dataagent.prompt.PromptConstant;
import com.alibaba.cloud.ai.dataagent.prompt.PromptHelper;
import com.alibaba.cloud.ai.dataagent.service.llm.LlmService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 计划生成节点（Planner）—— StateGraph 工作流中的"规划师"角色。
 *
 * <p>在工作流中的位置：IntentRecognition → EvidenceRecall → SchemaRecall → TableRelation
 * → FeasibilityAssessment → <b>Planner（本节点）</b> → [HumanFeedback] → PlanExecutor
 *
 * <p>核心职责：
 * <ul>
 *   <li>将用户的自然语言需求拆解为一个有序的执行计划（{@link Plan}），每一步指定使用什么工具（SQL/Python/报告生成）及其参数</li>
 *   <li>若开启了"人工反馈"模式，生成的计划可被人工修改后重新提交，此时本节点会携带历史计划与用户反馈重新生成</li>
 *   <li>若为纯 NL2SQL 模式（IS_ONLY_NL2SQL=true），则跳过 LLM 调用，直接返回一个仅含单步 SQL 执行的默认计划</li>
 * </ul>
 *
 * <p>涉及的 AI 概念：
 * <ul>
 *   <li><b>Prompt 模板渲染</b>：将用户问题、Schema 元数据、语义模型、RAG 证据等拼入 Prompt，交给 LLM</li>
 *   <li><b>结构化输出（Structured Output）</b>：通过 {@link BeanOutputConverter} 要求 LLM 返回符合 {@link Plan} 结构的 JSON</li>
 *   <li><b>Flux 流式响应</b>：LLM 的输出通过 Reactor Flux 流式返回给前端，实现 SSE 逐 token 推送</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see Plan 执行计划的数据结构
 * @see LlmService LLM 调用抽象层
 */
@Slf4j
@Component
@AllArgsConstructor
public class PlannerNode implements NodeAction {

	private final LlmService llmService;

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到此节点时自动调用。
	 *
	 * <p><b>输入状态读取：</b>
	 * <ul>
	 *   <li>{@code IS_ONLY_NL2SQL} — 是否为纯 NL2SQL 模式，跳过计划生成</li>
	 * </ul>
	 *
	 * <p><b>核心逻辑：</b>
	 * <ol>
	 *   <li>判断是否为纯 NL2SQL 模式，若是则直接返回固定计划</li>
	 *   <li>否则调用 LLM 根据用户问题 + Schema + 证据生成执行计划</li>
	 *   <li>用 JSON 标记符（```json ... ```）包裹 LLM 输出，便于前端解析</li>
	 *   <li>将 Flux 流包装为 {@link StreamingOutput}，写入状态供下游节点消费</li>
	 * </ol>
	 *
	 * <p><b>输出状态写入：</b>
	 * <ul>
	 *   <li>{@code PLANNER_NODE_OUTPUT} — 包含 Flux&lt;GraphResponse&lt;StreamingOutput&gt;&gt; 的流式生成器，
	 *       下游 PlanExecutor 节点会从中提取 {@link Plan} JSON</li>
	 * </ul>
	 *
	 * @param StateGraph 的全局状态对象，所有节点通过它共享数据
	 * @return 包含 PLANNER_NODE_OUTPUT 键的 Map，值为 Flux 流式生成器
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {
		// 读取"是否为纯 NL2SQL 模式"标记。纯 NL2SQL 模式下不需要 LLM 做规划，直接走固定单步 SQL 计划
		Boolean onlyNl2sql = state.value(IS_ONLY_NL2SQL, false);

		// 根据模式选择不同的 Flux 流：纯 NL2SQL 直接返回固定计划 JSON，否则调用 LLM 动态生成
		Flux<ChatResponse> flux = onlyNl2sql ? handleNl2SqlOnly() : handlePlanGenerate(state);

		// 用 JSON 类型标记符包裹 LLM 输出（如 ```json ... ```），前端据此识别内容类型并高亮展示
		Flux<ChatResponse> chatResponseFlux = Flux.concat(
				Flux.just(ChatResponseUtil.createPureResponse(TextType.JSON.getStartSign())), flux,
				Flux.just(ChatResponseUtil.createPureResponse(TextType.JSON.getEndSign())));

		// 将 ChatResponse Flux 转换为 GraphResponse<StreamingOutput> Flux，同时通过回调函数
		// 在流完成时提取去掉 JSON 标记符后的纯 JSON 字符串，写入状态供下游节点消费
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, v -> Map.of(PLANNER_NODE_OUTPUT, v.substring(TextType.JSON.getStartSign().length(),
						v.length() - TextType.JSON.getEndSign().length())),
				chatResponseFlux);

		return Map.of(PLANNER_NODE_OUTPUT, generator);
	}

	/**
	 * 调用 LLM 生成执行计划的核心方法。
	 *
	 * <p>Prompt 构建策略：将以下信息注入模板，让 LLM 理解数据库结构和用户意图：
	 * <ul>
	 *   <li><b>user_question</b>：经过 QueryEnhance 节点增强后的用户问题</li>
	 *   <li><b>schema</b>：由 SchemaRecall + TableRelation 节点筛选出的相关表结构（DDL）</li>
	 *   <li><b>evidence</b>：由 EvidenceRecall（RAG）节点检索到的相关知识片段</li>
	 *   <li><b>semantic_model</b>：语义层模型描述（业务术语与物理表的映射关系）</li>
	 *   <li><b>plan_validation_error</b>：如果上一轮计划被人工驳回，此处携带用户反馈</li>
	 *   <li><b>format</b>：BeanOutputConverter 生成的 JSON Schema 约束，要求 LLM 按 {@link Plan} 结构输出</li>
	 * </ul>
	 *
	 * @param state 全局状态对象
	 * @return LLM 响应的 Flux 流，内容为 Plan 结构的 JSON 字符串
	 */
	private Flux<ChatResponse> handlePlanGenerate(OverAllState state) {
		// 获取经过 QueryEnhance 节点增强/改写后的用户问题（canonicalQuery 是标准化后的查询文本）
		String canonicalQuery = StateUtil.getCanonicalQuery(state);
		log.info("Using processed query for planning: {}", canonicalQuery);

		// 检查是否为"计划修复"模式：如果 PLAN_VALIDATION_ERROR 非空，说明上一轮计划被人工驳回，
		// 需要携带用户反馈重新生成。这是 "Human-in-the-Loop" 机制的关键环节
		String validationError = StateUtil.getStringValue(state, PLAN_VALIDATION_ERROR, null);
		if (validationError != null) {
			log.info("Regenerating plan with user feedback: {}", validationError);
		}
		else {
			log.info("Generating initial plan");
		}

		// 读取语义层模型描述——业务术语与物理表/字段的映射关系，帮助 LLM 理解"收入"对应哪个表的哪个字段
		String semanticModel = (String) state.value(GENEGRATED_SEMANTIC_MODEL_PROMPT).orElse("");
		// 获取 SchemaRecall + TableRelation 节点筛选出的相关数据库表结构（DDL）
		SchemaDTO schemaDTO = StateUtil.getObjectValue(state, TABLE_RELATION_OUTPUT, SchemaDTO.class);
		// 将 SchemaDTO 转换为 LLM 可理解的文本格式（包含表名、字段、注释、外键关系等）
		String schemaStr = PromptHelper.buildMixMacSqlDbPrompt(schemaDTO, true);

		// 构建用户提示：如果是修复模式，会将历史计划和用户反馈一并拼入，引导 LLM 修正计划
		String userPrompt = buildUserPrompt(canonicalQuery, validationError, state);
		// 获取 RAG 检索到的证据（相关知识库文档片段），作为 LLM 生成计划的参考依据
		String evidence = StateUtil.getStringValue(state, EVIDENCE);

		// BeanOutputConverter 是 Spring AI 的结构化输出工具：它根据 Plan 类的注解生成 JSON Schema，
		// 约束 LLM 必须按此格式返回结果。getFormat() 返回的 JSON Schema 指令会拼入 Prompt 末尾
		BeanOutputConverter<Plan> beanOutputConverter = new BeanOutputConverter<>(Plan.class);
		Map<String, Object> params = Map.of("user_question", userPrompt, "schema", schemaStr, "evidence", evidence,
				"semantic_model", semanticModel, "plan_validation_error", formatValidationError(validationError),
				"format", beanOutputConverter.getFormat());
		// 使用 Prompt 模板引擎渲染最终 Prompt，将上述所有参数填入模板占位符
		String plannerPrompt = PromptConstant.getPlannerPromptTemplate().render(params);
		log.debug("Planner prompt: as follows \n{}\n", plannerPrompt);

		// 以"用户消息"角色调用 LLM（callUser 内部会将 prompt 作为 user 角色发送给大模型）
		// 返回的 Flux<ChatResponse> 是流式响应，每个 ChatResponse 包含 LLM 生成的一小段 token
		return llmService.callUser(plannerPrompt);
	}

	/**
	 * 纯 NL2SQL 模式下的快捷路径：跳过 LLM 规划，直接返回一个只包含单步 SQL 执行的固定计划。
	 *
	 * <p>当用户只需要简单的"自然语言转 SQL"而不需要多步骤分析时，
	 * IS_ONLY_NL2SQL 标记会被设为 true，本方法构造一个预定义的计划 JSON，避免不必要的 LLM 调用。
	 *
	 * @return 包含固定 NL2SQL 计划的 Flux 流
	 */
	private Flux<ChatResponse> handleNl2SqlOnly() {
		// Plan.nl2SqlPlan() 返回一个仅含"sql_generate → sql_execute"单步的固定 Plan JSON
		return Flux.just(ChatResponseUtil.createPureResponse(Plan.nl2SqlPlan()));
	}

	/**
	 * 构建用户提示，区分"首次生成"和"修复模式"两种场景。
	 *
	 * <p>修复模式下，会将以下信息拼入提示词，帮助 LLM 理解上下文并修正计划：
	 * <ul>
	 *   <li>用户对上一轮计划的驳回反馈</li>
	 *   <li>原始用户问题</li>
	 *   <li>上一轮被驳回的计划内容</li>
	 * </ul>
	 *
	 * @param input 经过增强的用户问题
	 * @param validationError 人工反馈内容（null 表示首次生成）
	 * @param state 全局状态，用于读取上一轮计划
	 * @return 最终拼装好的用户提示文本
	 */
	private String buildUserPrompt(String input, String validationError, OverAllState state) {
		if (validationError == null) {
			return input;
		}

		// 从状态中取出上一轮被驳回的计划 JSON，连同用户反馈一起交给 LLM
		String previousPlan = StateUtil.getStringValue(state, PLANNER_NODE_OUTPUT, "");
		return String.format(
				"IMPORTANT: User rejected previous plan with feedback: \"%s\"\n\n" + "Original question: %s\n\n"
						+ "Previous rejected plan:\n%s\n\n"
						+ "CRITICAL: Generate new plan incorporating user feedback (\"%s\")",
				validationError, input, previousPlan, validationError);
	}

	/**
	 * 将用户反馈格式化为 Prompt 模板中的 plan_validation_error 变量。
	 *
	 * <p>使用加粗标记和 "CRITICAL" 关键字提升 LLM 对反馈内容的注意力权重，
	 * 这是一种常见的 Prompt Engineering 技巧——通过强调语气引导 LLM 优先处理某些信息。
	 */
	private String formatValidationError(String validationError) {
		return validationError != null ? String
			.format("**USER FEEDBACK (CRITICAL)**: %s\n\n**Must incorporate this feedback.**", validationError) : "";
	}

}
