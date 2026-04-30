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

import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.util.JsonParseUtil;
import com.alibaba.cloud.ai.dataagent.properties.CodeExecutorProperties;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.dataagent.service.code.CodePoolExecutorService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.JsonUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * Python代码执行节点 —— StateGraph工作流中"深度分析"阶段的核心执行单元。
 *
 * <h3>工作流位置</h3>
 * <p>位于 {@code PythonGenerateNode}（LLM生成Python代码）之后、{@code ReportGeneratorNode}（报告生成）之前。
 * 工作流中的调用链为：{@code PlannerNode → PythonGenerateNode → <b>PythonExecuteNode</b> → ReportGeneratorNode}。</p>
 *
 * <h3>核心职责</h3>
 * <p>接收上游节点 {@code PythonGenerateNode} 生成的Python代码，将其提交到隔离的执行环境中运行（支持Docker容器、
 * 本地进程、AI模拟三种模式），并将执行结果（stdout的JSON输出）写入工作流状态，供下游节点消费。</p>
 *
 * <h3>涉及的AI/工程概念</h3>
 * <ul>
 *   <li><b>Code Interpreter模式</b>：LLM生成代码 → 沙箱执行 → 结果回传，是AI Agent常用的"工具调用"范式之一。
 *       与OpenAI Code Interpreter思路类似，但此处使用自建容器池（{@link CodePoolExecutorService}）。</li>
 *   <li><b>容器池化执行</b>：通过 {@link CodePoolExecutorService} 维护一组预热的Docker容器，
 *       避免每次执行都冷启动新容器，提高响应速度。配置参见 {@link CodeExecutorProperties}。</li>
 *   <li><b>降级兜底（Fallback）</b>：当代码执行失败且超过最大重试次数时，不终止整个工作流，
 *       而是标记 {@code PYTHON_FALLBACK_MODE=true}，让下游节点感知并输出降级提示。</li>
 *   <li><b>Flux流式输出</b>：通过 Reactor 的 {@link Flux} 将执行进度和结果以SSE（Server-Sent Events）形式
 *       实时推送给前端，实现"打字机效果"的用户体验。</li>
 * </ul>
 *
 * <h3>状态读取与写入</h3>
 * <ul>
 *   <li><b>读取</b>：{@code PYTHON_GENERATE_NODE_OUTPUT}（上游生成的Python代码）、
 *       {@code SQL_RESULT_LIST_MEMORY}（SQL查询结果，作为Python的输入数据）、
 *       {@code PYTHON_TRIES_COUNT}（已重试次数）</li>
 *   <li><b>写入</b>：{@code PYTHON_EXECUTE_NODE_OUTPUT}（执行结果JSON / 错误消息）、
 *       {@code PYTHON_IS_SUCCESS}（是否执行成功）、{@code PYTHON_FALLBACK_MODE}（是否进入降级模式）</li>
 * </ul>
 *
 * @author vlsmb
 * @since 2025/7/29
 * @see CodePoolExecutorService 容器池执行服务，支持Docker/Local/AI-Sim三种实现
 * @see CodeExecutorProperties 执行器配置（容器数、超时、重试次数等）
 */
@Slf4j
@Component
public class PythonExecuteNode implements NodeAction {

	private final CodePoolExecutorService codePoolExecutor;

	private final ObjectMapper objectMapper;

	private final JsonParseUtil jsonParseUtil;

	private final CodeExecutorProperties codeExecutorProperties;

	public PythonExecuteNode(CodePoolExecutorService codePoolExecutor, JsonParseUtil jsonParseUtil,
			CodeExecutorProperties codeExecutorProperties) {
		this.codePoolExecutor = codePoolExecutor;
		this.objectMapper = JsonUtil.getObjectMapper();
		this.jsonParseUtil = jsonParseUtil;
		this.codeExecutorProperties = codeExecutorProperties;
	}

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到该节点时自动调用。
	 *
	 * <p><b>执行流程</b>：</p>
	 * <ol>
	 *   <li>从工作流状态中读取上游生成的Python代码和SQL查询结果</li>
	 *   <li>构建任务请求，将代码和数据通过stdin传入容器执行</li>
	 *   <li>根据执行结果分三种情况处理：
	 *     <ul>
	 *       <li>成功：解析stdout为JSON，写入状态并推送成功消息</li>
	 *       <li>失败但未超重试上限：抛出异常，由上层重试机制（PlanExecutor）捕获并重新执行</li>
	 *       <li>失败且超重试上限：进入降级模式，设置 {@code PYTHON_FALLBACK_MODE=true}，工作流继续</li>
	 *     </ul>
	 *   </li>
	 * </ol>
	 *
	 * <p><b>返回值</b>：返回一个 Map，key 为状态字段名，value 为 {@code Flux<GraphResponse<StreamingOutput>>}。
	 * FluxUtil 会将这个 Flux 包装为 SSE 流推送给前端，同时在流结束后将实际结果写入 OverAllState。</p>
	 *
	 * @param state 当前工作流的全局共享状态对象，可读写各节点的输入输出
	 * @return 需要更新到状态中的字段映射，value 为 Flux 流（流式输出完毕后自动写入状态）
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		try {
			// ---- 1. 从状态中读取上游节点产生的Python代码和SQL查询结果 ----
			// PYTHON_GENERATE_NODE_OUTPUT：由 PythonGenerateNode 通过 LLM 生成的 Python 代码
			String pythonCode = StateUtil.getStringValue(state, PYTHON_GENERATE_NODE_OUTPUT);
			// SQL_RESULT_LIST_MEMORY：前面 SqlExecuteNode 执行 SQL 后获得的查询结果列表，
			// 将作为 Python 程序的输入数据，通过 stdin 传递给容器内的 Python 进程
			List<Map<String, String>> sqlResults = StateUtil.hasValue(state, SQL_RESULT_LIST_MEMORY)
					? StateUtil.getListValue(state, SQL_RESULT_LIST_MEMORY) : new ArrayList<>();

			// 读取已重试次数，用于判断是否需要降级
			// 当 Python 执行失败时，PlanExecutor 会递增此计数并重新进入本节点
			int triesCount = StateUtil.getObjectValue(state, PYTHON_TRIES_COUNT, Integer.class, 0);

			// ---- 2. 构建执行任务请求 ----
			// TaskRequest 包含三个字段：code（Python代码）、input（stdin输入的JSON数据）、requirement（需求描述，此处未使用）
			// 数据通过 stdin 传递而非文件挂载或HTTP，Python 代码通过 sys.stdin.read() 读取 JSON，通过 print() 输出结果
			CodePoolExecutorService.TaskRequest taskRequest = new CodePoolExecutorService.TaskRequest(pythonCode,
					objectMapper.writeValueAsString(sqlResults), null);

			// ---- 3. 提交到容器池执行 ----
			// codePoolExecutor 的具体实现取决于配置：DOCKER（Docker容器池）、LOCAL（本地进程）、AI_SIM（AI模拟，用于测试）
			// runTask 是同步阻塞调用，会等待容器执行完成或超时
			CodePoolExecutorService.TaskResponse taskResponse = this.codePoolExecutor.runTask(taskRequest);
			if (!taskResponse.isSuccess()) {
				String errorMsg = "Python Execute Failed!\nStdOut: " + taskResponse.stdOut() + "\nStdErr: "
						+ taskResponse.stdErr() + "\nExceptionMsg: " + taskResponse.exceptionMsg();
				log.error(errorMsg);

				// ---- 降级逻辑：超过最大重试次数后不再抛异常，改为标记降级模式 ----
				// 这样工作流不会中断，下游节点（如 ReportGenerator）可以感知到降级并给出友好提示
				if (triesCount >= codeExecutorProperties.getPythonMaxTriesCount()) {
					log.error("Python执行失败且已超过最大重试次数（已尝试次数：{}），启动降级兜底逻辑。错误信息: {}", triesCount, errorMsg);

					// 降级时输出空JSON，下游节点通过 PYTHON_IS_SUCCESS=false 和 PYTHON_FALLBACK_MODE=true 识别
					String fallbackOutput = "{}";

					// 构建降级场景下的前端展示流：推送降级提示消息
					Flux<ChatResponse> fallbackDisplayFlux = Flux.create(emitter -> {
						emitter.next(ChatResponseUtil.createResponse("开始执行Python代码..."));
						emitter.next(ChatResponseUtil.createResponse("Python代码执行失败已超过最大重试次数，采用降级策略继续处理。"));
						emitter.complete();
					});

					// FluxUtil.createStreamingGeneratorWithMessages 将展示流和状态更新函数包装为统一的 GraphResponse Flux
					// 流被订阅后，前端通过 SSE 收到展示消息；流结束后，状态更新函数被调用，将结果写入 OverAllState
					Flux<GraphResponse<StreamingOutput>> fallbackGenerator = FluxUtil
						.createStreamingGeneratorWithMessages(this.getClass(), state,
								v -> Map.of(PYTHON_EXECUTE_NODE_OUTPUT, fallbackOutput, PYTHON_IS_SUCCESS, false,
										PYTHON_FALLBACK_MODE, true),
								fallbackDisplayFlux);

					return Map.of(PYTHON_EXECUTE_NODE_OUTPUT, fallbackGenerator);
				}

				// 未超重试上限，抛出异常让 PlanExecutor 的重试机制捕获并重新执行本节点
				throw new RuntimeException(errorMsg);
			}

			// ---- 4. 执行成功，解析并规范化输出 ----
			// Python 容器输出的 JSON 可能包含 Unicode 转义序列（如 中文），
			// 通过 tryConvertToObject 先反序列化为 Java 对象，再用 writeValueAsString 重新序列化，
			// 从而将 Unicode 转义还原为原始中文字符
			String stdout = taskResponse.stdOut();
			Object value = jsonParseUtil.tryConvertToObject(stdout, Object.class);
			if (value != null) {
				stdout = objectMapper.writeValueAsString(value);
			}
			String finalStdout = stdout;

			log.info("Python Execute Success! StdOut: {}", finalStdout);

			// ---- 5. 构建前端展示流 ----
			// 使用 TextType.JSON 的标记符号包裹输出内容，前端据此识别为 JSON 代码块并做语法高亮
			Flux<ChatResponse> displayFlux = Flux.create(emitter -> {
				emitter.next(ChatResponseUtil.createResponse("开始执行Python代码..."));
				emitter.next(ChatResponseUtil.createResponse("标准输出："));
				// createPureResponse 不走LLM内容渲染，直接输出原始文本（这里输出JSON块的开始标记）
				emitter.next(ChatResponseUtil.createPureResponse(TextType.JSON.getStartSign()));
				emitter.next(ChatResponseUtil.createResponse(finalStdout));
				emitter.next(ChatResponseUtil.createPureResponse(TextType.JSON.getEndSign()));
				emitter.next(ChatResponseUtil.createResponse("Python代码执行成功！"));
				emitter.complete();
			});

			// ---- 6. 将展示流和状态更新函数封装为统一的 GraphResponse Flux ----
			// 第二个参数（Function）定义了流结束后写入状态的内容：
			//   - PYTHON_EXECUTE_NODE_OUTPUT：Python执行结果JSON
			//   - PYTHON_IS_SUCCESS：标记执行成功
			Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(
					this.getClass(), state,
					v -> Map.of(PYTHON_EXECUTE_NODE_OUTPUT, finalStdout, PYTHON_IS_SUCCESS, true), displayFlux);

			return Map.of(PYTHON_EXECUTE_NODE_OUTPUT, generator);
		}
		catch (Exception e) {
			// ---- 异常处理：捕获非执行失败的意外异常（如JSON解析异常等） ----
			// 与"执行失败但可重试"不同，这类异常直接将错误信息写入状态，PYTHON_IS_SUCCESS=false
			String errorMessage = e.getMessage();
			log.error("Python Execute Exception: {}", errorMessage);

			Map<String, Object> errorResult = Map.of(PYTHON_EXECUTE_NODE_OUTPUT, errorMessage, PYTHON_IS_SUCCESS,
					false);

			// 构建错误场景下的前端展示流
			Flux<ChatResponse> errorDisplayFlux = Flux.create(emitter -> {
				emitter.next(ChatResponseUtil.createResponse("开始执行Python代码..."));
				emitter.next(ChatResponseUtil.createResponse("Python代码执行失败: " + errorMessage));
				emitter.complete();
			});

			var generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(), state, v -> errorResult,
					errorDisplayFlux);

			return Map.of(PYTHON_EXECUTE_NODE_OUTPUT, generator);
		}
	}

}
