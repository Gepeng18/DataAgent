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

import com.alibaba.cloud.ai.dataagent.dto.planner.ExecutionStep;
import com.alibaba.cloud.ai.dataagent.dto.planner.Plan;
import com.alibaba.cloud.ai.dataagent.entity.UserPromptConfig;
import com.alibaba.cloud.ai.dataagent.prompt.PromptHelper;
import com.alibaba.cloud.ai.dataagent.service.llm.LlmService;
import com.alibaba.cloud.ai.dataagent.service.prompt.UserPromptService;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 报告生成节点 —— StateGraph工作流中最后一个处理节点，负责将所有步骤的执行结果汇总为用户可读的分析报告。
 *
 * <h3>工作流位置</h3>
 * <p>位于 {@code PlanExecutorNode}（计划执行器，循环执行SQL/Python步骤）之后，是整个工作流的终端输出节点。
 * 工作流调用链：{@code PlannerNode → PlanExecutorNode → <b>ReportGeneratorNode</b>}。</p>
 *
 * <h3>核心职责</h3>
 * <ul>
 *   <li>从工作流状态中提取执行计划（{@link Plan}）、各步骤的SQL执行结果和Python分析结果</li>
 *   <li>构建结构化的Prompt，将"用户需求 + 执行计划 + 数据结果 + 优化配置"组合后发送给LLM</li>
 *   <li>LLM以流式方式生成 Markdown 格式的分析报告，支持内嵌 ECharts 图表代码块</li>
 *   <li>将最终报告内容写入状态的 {@code RESULT} 字段，同时清空中间过程状态</li>
 * </ul>
 *
 * <h3>涉及的AI/工程概念</h3>
 * <ul>
 *   <li><b>Prompt工程</b>：本节点通过 {@link PromptHelper#buildReportGeneratorPromptWithOptimization}
 *       构建报告生成Prompt，将结构化数据转换为LLM可理解的文本描述。Prompt模板支持用户自定义优化配置
 *       （{@link UserPromptConfig}），允许针对特定Agent定制报告风格。</li>
 *   <li><b>LLM流式输出（Streaming）</b>：通过 {@link LlmService#callUser} 获取 Flux 流，
 *       LLM逐token生成报告内容，前端通过SSE实时接收并渲染，实现"打字机效果"。</li>
 *   <li><b>ECharts图表嵌入</b>：报告中可包含 {@code ```echarts} 代码块，前端解析后在页面中渲染
 *       交互式图表（柱状图、折线图、饼图等），这是"AI+数据可视化"的典型实现方式。</li>
 *   <li><b>BeanOutputConverter</b>：Spring AI 提供的 JSON→Java 对象转换器，
 *       利用 LLM 的结构化输出能力（Function Calling / JSON Mode）将 Plan JSON 解析为 Java POJO。</li>
 * </ul>
 *
 * <h3>状态读取与写入</h3>
 * <ul>
 *   <li><b>读取</b>：{@code PLANNER_NODE_OUTPUT}（执行计划JSON）、{@code PLAN_CURRENT_STEP}（当前步骤）、
 *       {@code SQL_EXECUTE_NODE_OUTPUT}（各步骤的执行结果Map）、{@code AGENT_ID}（智能体ID，用于加载定制配置）</li>
 *   <li><b>写入</b>：{@code RESULT}（最终报告内容）、同时将 {@code SQL_EXECUTE_NODE_OUTPUT}、
 *       {@code PLAN_CURRENT_STEP}、{@code PLANNER_NODE_OUTPUT} 置为 null（清理中间状态）</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see PromptHelper#buildReportGeneratorPromptWithOptimization 报告Prompt构建方法
 * @see LlmService LLM调用服务，支持流式和阻塞两种模式
 * @see UserPromptService 用户自定义Prompt配置服务
 */
@Slf4j
@Component
public class ReportGeneratorNode implements NodeAction {

	private final LlmService llmService;

	private final BeanOutputConverter<Plan> converter;

	private final UserPromptService promptConfigService;

	public ReportGeneratorNode(LlmService llmService, UserPromptService promptConfigService) {
		this.llmService = llmService;
		this.converter = new BeanOutputConverter<>(new ParameterizedTypeReference<>() {
		});
		this.promptConfigService = promptConfigService;
	}

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到该节点时自动调用。
	 *
	 * <p><b>执行流程</b>：</p>
	 * <ol>
	 *   <li>从工作流状态中读取执行计划JSON、用户原始查询、当前步骤和各步骤执行结果</li>
	 *   <li>解析执行计划，提取当前步骤的"总结与建议"字段（由 PlannerNode 中 LLM 预生成的报告指导信息）</li>
	 *   <li>加载当前 Agent 的 Prompt 优化配置（如果有的话）</li>
	 *   <li>构建报告生成 Prompt 并调用 LLM 以流式方式生成 Markdown 报告</li>
	 *   <li>通过 FluxUtil 将 LLM 流式输出包装为 GraphResponse Flux，前端通过 SSE 实时接收</li>
	 *   <li>流结束后将报告内容写入 RESULT 字段，并清理中间过程状态</li>
	 * </ol>
	 *
	 * @param state 当前工作流的全局共享状态对象
	 * @return 包含 {@code RESULT} 键的 Map，value 为 Flux 流（流式输出完毕后自动将报告内容写入状态）
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// ---- 1. 从状态中读取所需数据 ----
		// PLANNER_NODE_OUTPUT：PlannerNode 生成的执行计划 JSON 字符串，包含完整的执行步骤定义
		String plannerNodeOutput = StateUtil.getStringValue(state, PLANNER_NODE_OUTPUT);
		// getCanonicalQuery：获取规范化后的用户查询（可能经过 QueryEnhanceNode 增强）
		String userInput = StateUtil.getCanonicalQuery(state);
		// 当前正在执行的步骤编号（从1开始）
		Integer currentStep = StateUtil.getObjectValue(state, PLAN_CURRENT_STEP, Integer.class, 1);
		// SQL_EXECUTE_NODE_OUTPUT：一个 HashMap，key 为 "step_1", "step_2" 等，
		// value 为对应步骤的 SQL 执行结果 JSON；还可能包含 "step_N_analysis" 形式的 Python 分析结果
		@SuppressWarnings("unchecked")
		HashMap<String, String> executionResults = StateUtil.getObjectValue(state, SQL_EXECUTE_NODE_OUTPUT,
				HashMap.class, new HashMap<>());

		// ---- 2. 解析执行计划并提取报告指导信息 ----
		// BeanOutputConverter 利用 LLM 的结构化输出能力，将 JSON 字符串反序列化为 Plan POJO
		Plan plan = converter.convert(plannerNodeOutput);
		ExecutionStep executionStep = getCurrentExecutionStep(plan, currentStep);
		// summaryAndRecommendations 是 PlannerNode 在规划阶段为报告节点预生成的指导信息，
		// 描述了该步骤应该产出什么样的分析结论和建议
		String summaryAndRecommendations = executionStep.getToolParameters().getSummaryAndRecommendations();

		// ---- 3. 获取 Agent ID，用于加载该 Agent 的定制 Prompt 配置 ----
		// 不同 Agent 可以有不同的报告风格、输出格式要求等
		String agentIdStr = StateUtil.getStringValue(state, AGENT_ID);
		Long agentId = null;
		try {
			if (agentIdStr != null) {
				agentId = Long.parseLong(agentIdStr);
			}
		}
		catch (NumberFormatException ignore) {
			// 解析失败时 agentId 为 null，后续将使用全局默认配置
		}

		// ---- 4. 构建 Prompt 并调用 LLM 生成报告（流式） ----
		Flux<ChatResponse> reportGenerationFlux = generateReport(userInput, plan, executionResults,
				summaryAndRecommendations, agentId);

		// 使用 MARK_DOWN 类型标记，前端据此渲染为 Markdown 格式
		TextType reportTextType = TextType.MARK_DOWN;

		// ---- 5. 封装为统一的 GraphResponse Flux ----
		// FluxUtil.createStreamingGeneratorWithMessages 的参数说明：
		//   - "开始生成报告..."：流开始时推送的前置提示消息
		//   - "报告生成完成！"：流结束时推送的后置提示消息
		//   - reportContent -> {...}：内容收集回调，流结束后将聚合的完整报告内容写入状态
		//     同时将中间状态字段置 null，避免影响后续可能的多次对话
		//   - 最后一个参数是实际的展示流：Markdown 标记 + LLM 流式输出 + Markdown 结束标记
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, "开始生成报告...", "报告生成完成！", reportContent -> {
					log.info("Generated report content: {}", reportContent);
					Map<String, Object> result = new HashMap<>();
					result.put(RESULT, reportContent);
					// 清理中间过程状态，这些字段已完成使命，置 null 防止干扰后续对话轮次
					result.put(SQL_EXECUTE_NODE_OUTPUT, null);
					result.put(PLAN_CURRENT_STEP, null);
					result.put(PLANNER_NODE_OUTPUT, null);
					return result;
				},
				Flux.concat(Flux.just(ChatResponseUtil.createPureResponse(reportTextType.getStartSign())),
						reportGenerationFlux,
						Flux.just(ChatResponseUtil.createPureResponse(reportTextType.getEndSign()))));

		return Map.of(RESULT, generator);
	}

	/**
	 * Gets the current execution step from the plan.
	 */
	private ExecutionStep getCurrentExecutionStep(Plan plan, Integer currentStep) {
		List<ExecutionStep> executionPlan = plan.getExecutionPlan();
		if (executionPlan == null || executionPlan.isEmpty()) {
			throw new IllegalStateException("Execution plan is empty");
		}

		int stepIndex = currentStep - 1;
		if (stepIndex < 0 || stepIndex >= executionPlan.size()) {
			throw new IllegalStateException("Current step index out of range: " + stepIndex);
		}

		return executionPlan.get(stepIndex);
	}

	/**
	 * 构建报告生成 Prompt 并调用 LLM 以流式方式生成报告。
	 *
	 * <p>Prompt 由以下几部分拼接而成：</p>
	 * <ol>
	 *   <li><b>用户需求与计划概述</b>：用户原始问题 + LLM的思考过程 + 各步骤描述</li>
	 *   <li><b>数据执行结果</b>：每个步骤的SQL执行结果、Python分析结果，以结构化Markdown呈现</li>
	 *   <li><b>总结与建议</b>：PlannerNode 预生成的报告指导信息</li>
	 *   <li><b>优化配置</b>：用户自定义的 Prompt 补充内容（按 Agent 粒度配置）</li>
	 * </ol>
	 *
	 * @param userInput 用户原始查询文本
	 * @param plan 解析后的执行计划对象
	 * @param executionResults 各步骤的执行结果Map，key为"step_N"，value为JSON结果
	 * @param summaryAndRecommendations 报告指导信息（由PlannerNode预生成）
	 * @param agentId 智能体ID，用于加载该Agent的定制Prompt配置；null时使用全局配置
	 * @return LLM流式输出的 Flux，每个 ChatResponse 包含一段生成的文本片段
	 */
	private Flux<ChatResponse> generateReport(String userInput, Plan plan, HashMap<String, String> executionResults,
			String summaryAndRecommendations, Long agentId) {
		// 将用户需求和执行计划拼接为 Markdown 格式的描述文本
		String userRequirementsAndPlan = buildUserRequirementsAndPlan(userInput, plan);

		// 将各步骤的执行结果（SQL结果 + Python分析结果）拼接为 Markdown 格式的数据描述
		String analysisStepsAndData = buildAnalysisStepsAndData(plan, executionResults);

		// 加载 Prompt 优化配置：优先按 Agent 维度加载，如果该 Agent 没有配置则使用全局默认配置
		// 优化配置允许用户在不修改代码的情况下定制报告风格、输出格式等
		List<UserPromptConfig> optimizationConfigs = promptConfigService.getOptimizationConfigs("report-generator",
				agentId);

		// 通过 PromptHelper 使用模板引擎（Mustache）渲染最终的报告生成 Prompt
		// 模板中包含了 ECharts JSON 示例，引导 LLM 在报告中内嵌 ```echarts 代码块以生成可视化图表
		String reportPrompt = PromptHelper.buildReportGeneratorPromptWithOptimization(userRequirementsAndPlan,
				analysisStepsAndData, summaryAndRecommendations, optimizationConfigs);
		log.debug("Report Node Prompt: \n {} \n", reportPrompt);

		// 调用 LLM 服务，以流式（Streaming）模式获取报告内容
		// callUser 返回的 Flux 会在 LLM 逐 token 生成时持续发射 ChatResponse
		return llmService.callUser(reportPrompt);
	}

	/**
	 * 将用户原始需求和执行计划拼接为 Markdown 格式文本，作为报告 Prompt 的一部分。
	 *
	 * <p>输出的内容包括：用户原始需求、LLM的思考过程（Chain-of-Thought）、以及每个执行步骤的工具和参数描述。
	 * 这些上下文信息帮助 LLM 理解整个分析过程，从而生成更准确、更连贯的报告。</p>
	 *
	 * @param userInput 用户原始查询
	 * @param plan 解析后的执行计划对象，包含思考过程和步骤列表
	 * @return Markdown 格式的需求与计划描述文本
	 */
	private String buildUserRequirementsAndPlan(String userInput, Plan plan) {
		StringBuilder sb = new StringBuilder();
		sb.append("## 用户原始需求\n");
		sb.append(userInput).append("\n\n");

		sb.append("## 执行计划概述\n");
		sb.append("**思考过程**: ").append(plan.getThoughtProcess()).append("\n\n");

		sb.append("## 详细执行步骤\n");
		List<ExecutionStep> executionPlan = plan.getExecutionPlan();
		for (int i = 0; i < executionPlan.size(); i++) {
			ExecutionStep step = executionPlan.get(i);
			sb.append("### 步骤 ").append(i + 1).append(": 步骤编号 ").append(step.getStep()).append("\n");
			sb.append("**工具**: ").append(step.getToolToUse()).append("\n");
			if (step.getToolParameters() != null) {
				sb.append("**参数描述**: ").append(step.getToolParameters().getInstruction()).append("\n");
			}
			sb.append("\n");
		}

		return sb.toString();
	}

	/**
	 * 将各步骤的执行结果拼接为 Markdown 格式文本，作为报告 Prompt 的一部分。
	 *
	 * <p>对于每个步骤，会输出：步骤编号、使用的工具（sql_query / python_code 等）、参数描述、
	 * 执行的 SQL 语句、SQL 执行结果（JSON）、以及 Python 分析结果（如果有）。
	 * 这些数据是 LLM 生成报告的核心依据。</p>
	 *
	 * @param plan 解析后的执行计划对象
	 * @param executionResults 各步骤的执行结果Map，key 格式为 "step_N" 或 "step_N_analysis"
	 * @return Markdown 格式的数据结果描述文本
	 */
	private String buildAnalysisStepsAndData(Plan plan, HashMap<String, String> executionResults) {
		StringBuilder sb = new StringBuilder();
		sb.append("## 数据执行结果\n");

		if (executionResults.isEmpty()) {
			sb.append("暂无执行结果数据\n");
		}
		else {
			List<ExecutionStep> executionPlan = plan.getExecutionPlan();
			for (int i = 0; i < executionPlan.size(); i++) {
				ExecutionStep step = executionPlan.get(i);
				String stepId = String.valueOf(i + 1);
				String stepKey = "step_" + stepId;
				String stepResult = executionResults.get(stepKey);
				String analysisResult = executionResults.get(stepKey + "_analysis");

				if ((stepResult == null || stepResult.trim().isEmpty())
						&& (analysisResult == null || analysisResult.trim().isEmpty())) {
					continue;
				}

				sb.append("### ").append(stepKey).append("\n");
				sb.append("**步骤编号**: ").append(step.getStep()).append("\n");
				sb.append("**使用工具**: ").append(step.getToolToUse()).append("\n");
				if (step.getToolParameters() != null) {
					sb.append("**参数描述**: ").append(step.getToolParameters().getInstruction()).append("\n");
					if (step.getToolParameters().getSqlQuery() != null) {
						sb.append("**执行SQL**: \n```sql\n")
							.append(step.getToolParameters().getSqlQuery())
							.append("\n```\n");
					}
				}

				if (stepResult != null && !stepResult.trim().isEmpty()) {
					sb.append("**执行结果**: \n```json\n").append(stepResult).append("\n```\n\n");
				}
				if (analysisResult != null && !analysisResult.trim().isEmpty()) {
					sb.append("**Python 分析结果**: ").append(analysisResult).append("\n\n");
				}
			}
		}

		return sb.toString();
	}

}
