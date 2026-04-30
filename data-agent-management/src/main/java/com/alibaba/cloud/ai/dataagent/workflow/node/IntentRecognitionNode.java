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

import com.alibaba.cloud.ai.dataagent.dto.prompt.IntentRecognitionOutputDTO;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.util.JsonParseUtil;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.dataagent.prompt.PromptHelper;
import com.alibaba.cloud.ai.dataagent.service.llm.LlmService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 意图识别节点 —— StateGraph 工作流的第一个处理节点。
 *
 * <h3>在工作流中的位置</h3>
 * <p>用户提问 → <b>IntentRecognitionNode（当前节点）</b> → EvidenceRecallNode → SchemaRecallNode → ... → 报告生成</p>
 *
 * <h3>核心职责</h3>
 * <p>判断用户输入属于"闲聊/无关指令"还是"可能的数据分析请求"。</p>
 * <p>设计遵循"宁放过不杀错"原则：只要有一丝可能是数据分析请求就放行，只有明确闲聊才拦截，
 *    避免误拒用户的正常分析需求。</p>
 *
 * <h3>涉及的 AI 概念</h3>
 * <ul>
 *   <li><b>意图分类 (Intent Classification)</b>：LLM 充当分类器，将用户输入映射到预定义的意图类别。
 *       传统做法是训练专门的分类模型，而这里利用大语言模型的通用理解能力，
 *       通过 Prompt Engineering 实现 zero-shot 分类，无需额外训练数据。</li>
 *   <li><b>结构化输出 (Structured Output)</b>：要求 LLM 以 JSON 格式返回分类结果，
 *       通过 {@link IntentRecognitionOutputDTO} 的 @JsonProperty 注解约束输出结构，
 *       保证下游节点可以可靠解析。</li>
 *   <li><b>流式推理 (Streaming Inference)</b>：LLM 的推理过程是逐 token 生成的，
 *       使用 Reactor Flux 将中间结果实时推送到前端（SSE），用户可看到"正在进行意图识别..."等进度提示。</li>
 * </ul>
 *
 * @see NodeAction StateGraph 节点的统一接口
 * @see LlmService LLM 调用服务
 * @see IntentRecognitionOutputDTO 意图分类结果的 DTO
 */
@Slf4j
@Component
@AllArgsConstructor
public class IntentRecognitionNode implements NodeAction {

	private final LlmService llmService;

	private final JsonParseUtil jsonParseUtil;

	/**
	 * 节点执行入口，由 StateGraph 框架在到达该节点时自动调用。
	 *
	 * <h3>输入状态读取</h3>
	 * <ul>
	 *   <li>{@code INPUT_KEY} —— 用户原始提问文本</li>
	 *   <li>{@code MULTI_TURN_CONTEXT} —— 多轮对话上下文（由 MultiTurnContextManager 维护），用于辅助理解省略指代的意图</li>
	 * </ul>
	 *
	 * <h3>LLM 调用逻辑</h3>
	 * <ol>
	 *   <li>将用户提问 + 多轮上下文拼接为 Prompt，交由 LLM 进行意图分类</li>
	 *   <li>LLM 以 JSON 格式输出分类结果（classification 字段），在 Flux 流中逐 token 返回</li>
	 * </ol>
	 *
	 * <h3>输出状态写入</h3>
	 * <ul>
	 *   <li>{@code INTENT_RECOGNITION_NODE_OUTPUT} —— 意图分类结果（{@link IntentRecognitionOutputDTO}），
	 *       下游节点根据此结果决定是否继续执行数据分析流程</li>
	 * </ul>
	 *
	 * @param state StateGraph 的全局共享状态对象，节点之间通过它传递数据
	 * @return 包含 Flux 流式生成器的 Map，key 为状态字段名，value 为响应式流
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// 从全局共享状态中读取用户输入和多轮对话上下文
		String userInput = StateUtil.getStringValue(state, INPUT_KEY);
		log.info("User input for intent recognition: {}", userInput);

		// 多轮上下文：MultiTurnContextManager 会保留最近 N 轮的对话历史，
		// 用于处理用户说"帮我查一下"这种省略指代的情况
		String multiTurn = StateUtil.getStringValue(state, MULTI_TURN_CONTEXT, "(无)");

		// 构建意图识别的 Prompt，包含 system 指令 + 用户输入 + 多轮上下文
		// Prompt 模板定义了输出 JSON 的 schema（classification 字段）
		String prompt = PromptHelper.buildIntentRecognitionPrompt(multiTurn, userInput);
		log.debug("Built intent recognition prompt as follows \n {} \n", prompt);

		// "宁放过不杀错"原则：只要有一丝可能是数据分析请求就放行，只有明确闲聊才拦截
		// callUser() 以流式模式调用 LLM，返回 Flux<ChatResponse>，
		// 每个 ChatResponse 包含 LLM 生成的一个或多个 token
		Flux<ChatResponse> responseFlux = llmService.callUser(prompt);

		// FluxUtil.createStreamingGenerator 是一个通用的流式响应包装器，它完成以下工作：
		// 1. preFlux：在 LLM 输出前向前端推送进度提示（"正在进行意图识别..."）
		// 2. 包裹 LLM 的 JSON 输出前后添加标记符号（TextType.JSON 的起止标记），
		//    前端据此识别流式内容中哪部分是结构化 JSON
		// 3. postFlux：LLM 输出完成后推送结束提示
		// 4. resultMapper：将 LLM 完整输出字符串解析为 Java DTO 对象，作为状态写入
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGenerator(this.getClass(), state,
				responseFlux,
				Flux.just(ChatResponseUtil.createResponse("正在进行意图识别..."),
						ChatResponseUtil.createPureResponse(TextType.JSON.getStartSign())),
				Flux.just(ChatResponseUtil.createPureResponse(TextType.JSON.getEndSign()),
						ChatResponseUtil.createResponse("\n意图识别完成！")),
				result -> {
					// LLM 输出的完整文本是 JSON 字符串，如 {"classification": "《可能的数据分析请求》"}
					// jsonParseUtil.tryConvertToObject 使用 Jackson 将其反序列化为 DTO
					IntentRecognitionOutputDTO intentRecognitionOutput = jsonParseUtil.tryConvertToObject(result,
							IntentRecognitionOutputDTO.class);
					return Map.of(INTENT_RECOGNITION_NODE_OUTPUT, intentRecognitionOutput);
				});
		// 返回值以 Map 形式写入 StateGraph 的全局状态，
		// key = INTENT_RECOGNITION_NODE_OUTPUT，value = 响应式流生成器
		// 框架会在流消费完毕后将最终结果（DTO 对象）写入 state 供下游节点使用
		return Map.of(INTENT_RECOGNITION_NODE_OUTPUT, generator);
	}

}
