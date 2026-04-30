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
package com.alibaba.cloud.ai.dataagent.controller;

import com.alibaba.cloud.ai.dataagent.dto.GraphRequest;
import com.alibaba.cloud.ai.dataagent.service.graph.GraphService;
import com.alibaba.cloud.ai.dataagent.vo.GraphNodeResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.STREAM_EVENT_COMPLETE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.STREAM_EVENT_ERROR;

/**
 * 数据分析图（StateGraph）的流式交互控制器。
 *
 * <p>这是整个 DataAgent 系统面向前端的唯一流式入口。前端的每一次数据分析请求（自然语言提问、
 * 人工反馈、计划驳回等）都通过本控制器的 SSE（Server-Sent Events）端点接入后端的
 * StateGraph 工作流引擎。</p>
 *
 * <h3>核心概念说明：</h3>
 * <ul>
 *   <li><b>SSE（Server-Sent Events）</b>：一种基于 HTTP 的单向实时推送协议。服务端可以持续
 *       向浏览器推送文本事件流，前端通过 EventSource API 接收。与 WebSocket 不同，SSE 是
 *       单向的（服务端→客户端），更适合 LLM 逐步生成结果的场景。</li>
 *   <li><b>StateGraph 工作流</b>：后端使用 Spring AI Alibaba Graph 实现的有向图编排引擎。
 *       用户的一次查询会依次经过意图识别、Schema召回、规划、SQL/Python执行、报告生成等节点，
 *       每个节点的输出通过 SSE 实时推送回前端。</li>
 *   <li><b>Human-in-the-Loop（人工反馈）</b>：LLM 生成执行计划后可以暂停等待人工审核，
 *       用户确认或修改后再继续执行。这是 AI 系统中常见的人机协作模式。</li>
 *   <li><b>Flux / Sinks</b>：来自 Project Reactor（Spring WebFlux 的响应式编程基础）。
 *       Flux 是异步数据流（0..N 个元素的序列），Sinks 是手动向 Flux 推送数据的入口。
 *       本控制器通过 Sinks 桥接同步的图执行逻辑与异步的 HTTP 流式响应。</li>
 * </ul>
 *
 * <h3>与其它组件的关系：</h3>
 * <ul>
 *   <li>{@link GraphService} -- 实际执行 StateGraph 工作流的服务层，本控制器只负责 HTTP 接入和 SSE 协议适配</li>
 *   <li>{@link GraphRequest} -- 封装请求参数的 DTO，包含 agentId、查询文本、人工反馈等</li>
 *   <li>{@link GraphNodeResponse} -- 每个工作流节点的输出结构体，包含节点名称、文本内容、文本类型等</li>
 * </ul>
 *
 * @author zhangshenghang
 * @author vlsmb
 * @see GraphService#graphStreamProcess(Sinks.Many, GraphRequest)
 */
@Slf4j
@RestController
@AllArgsConstructor
@CrossOrigin(origins = "*")
@RequestMapping("/api")
public class GraphController {

	private final GraphService graphService;

	/**
	 * 流式数据分析入口（SSE 端点）。
	 *
	 * <p>这是整个系统最核心的 HTTP 端点。前端发起数据分析请求后，本方法：</p>
	 * <ol>
	 *   <li>创建一个 Reactor {@link Sinks.Many} 作为生产者-消费者管道</li>
	 *   <li>将 Sink 和请求参数交给 {@link GraphService} 异步执行 StateGraph 工作流</li>
	 *   <li>将 Sink 转为 {@link Flux} 返回给 Spring WebFlux 框架，由框架自动以 SSE 协议推送给客户端</li>
	 * </ol>
	 *
	 * <h3>SSE 事件流的生命周期示例：</h3>
	 * <pre>
	 * event: intent_recognition  data: {"text":"正在分析您的意图..."}
	 * event: schema_recall       data: {"text":"已召回3张相关表"}
	 * event: planner             data: {"text":"生成执行计划..."}
	 * event: sql_execute         data: {"text":"查询结果: ..."}
	 * event: complete            data: {}
	 * </pre>
	 *
	 * @param agentId              Agent（智能体）的唯一标识。DataAgent 支持多个独立配置的 Agent，
	 *                             每个 Agent 有自己的数据源、提示词模板、向量库等配置。
	 *                             通过 agentId 路由到对应的 Agent 实例。
	 * @param threadId             会话线程ID，用于标识一次多轮对话。首次请求可不传（由后端生成），
	 *                             后续请求（如人工反馈后恢复）必须传入相同的 threadId 以恢复上下文。
	 *                             底层通过 Spring AI Alibaba Graph 的 Checkpoint 机制持久化状态。
	 * @param query                用户的自然语言查询，例如"上个月销售额最高的前10个产品是什么"
	 * @param humanFeedback        是否为人工反馈请求。当 LLM 生成的执行计划需要人工审核时，
	 *                             StateGraph 会在 HumanFeedbackNode 处暂停（interruptBefore），
	 *                             用户通过将此参数设为 true 并携带反馈内容来恢复执行。
	 * @param humanFeedbackContent 人工反馈的具体内容，例如"请修改第2步的SQL条件"或"计划通过"
	 * @param rejectedPlan         是否驳回了执行计划。true 表示用户对计划不满意，LLM 需要重新规划
	 * @param nl2sqlOnly           是否仅执行 NL2SQL（自然语言转SQL）模式。true 时跳过 Python 分析、
	 *                             报告生成等步骤，只返回 SQL 查询结果，适用于简单的数据查询场景
	 * @param response             WebFlux 的响应对象，用于设置 SSE 必需的 HTTP 响应头
	 * @return {@link Flux}<{@link ServerSentEvent}<{@link GraphNodeResponse}>>
	 *         响应式流，Spring WebFlux 会将其自动转为 SSE 事件流推送给客户端。
	 *         每个 SSE 事件的 event 字段对应工作流节点名称，data 字段为节点输出内容。
	 */
	@GetMapping(value = "/stream/search", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<ServerSentEvent<GraphNodeResponse>> streamSearch(@RequestParam("agentId") String agentId,
			@RequestParam(value = "threadId", required = false) String threadId, @RequestParam("query") String query,
			@RequestParam(value = "humanFeedback", required = false) boolean humanFeedback,
			@RequestParam(value = "humanFeedbackContent", required = false) String humanFeedbackContent,
			@RequestParam(value = "rejectedPlan", required = false) boolean rejectedPlan,
			@RequestParam(value = "nl2sqlOnly", required = false) boolean nl2sqlOnly, ServerHttpResponse response) {

		// 设置 SSE 必需的 HTTP 响应头
		// "no-cache" 防止代理服务器或浏览器缓存事件流，确保每条消息都实时送达
		// "keep-alive" 保持 TCP 长连接，SSE 依赖持久连接实现服务端推送
		// "Access-Control-Allow-Origin: *" 允许跨域访问（前后端分离开发时必需）
		response.getHeaders().add("Cache-Control", "no-cache");
		response.getHeaders().add("Connection", "keep-alive");
		response.getHeaders().add("Access-Control-Allow-Origin", "*");

		// Sinks.Many 是 Reactor 提供的手动数据推送管道，用于桥接"同步/阻塞式生产者"与"异步响应式消费者"。
		//
		// 这里选用 unicast()（单播）+ onBackpressureBuffer()（背压缓冲）模式：
		//   - unicast：只允许一个订阅者（即当前这个 HTTP 连接），适用于 SSE 一对一场景
		//   - onBackpressureBuffer：当生产速度（图节点执行）快于消费速度（网络传输）时，
		//     在缓冲区中暂存未消费的事件，避免数据丢失
		//
		// 数据流向：graphService 内部各工作流节点 -> sink.tryEmitNext() -> 此 Flux -> SSE 推送到浏览器
		Sinks.Many<ServerSentEvent<GraphNodeResponse>> sink = Sinks.many().unicast().onBackpressureBuffer();

		// 构建请求 DTO，包含本次交互的全部上下文信息
		GraphRequest request = GraphRequest.builder()
			.agentId(agentId)
			.threadId(threadId)
			.query(query)
			.humanFeedback(humanFeedback)
			.humanFeedbackContent(humanFeedbackContent)
			.rejectedPlan(rejectedPlan)
			.nl2sqlOnly(nl2sqlOnly)
			.build();

		// 将 Sink 传递给服务层，服务层在执行 StateGraph 各节点时通过 sink.emitNext() 推送结果。
		// 注意：此方法内部是异步执行的（新线程或线程池），不会阻塞当前 HTTP 线程。
		graphService.graphStreamProcess(sink, request);

		// 将 Sink 转为 Flux 并添加过滤和生命周期回调
		return sink.asFlux().filter(sse -> {
				// 过滤掉"空文本"事件，避免向前端推送无意义的空白消息。
				// 但 complete 和 error 事件是终端信号，即使没有文本内容也必须放行，
				// 因为前端依赖这两个事件来关闭 EventSource 连接或展示错误提示。
				if (STREAM_EVENT_COMPLETE.equals(sse.event()) || STREAM_EVENT_ERROR.equals(sse.event())) {
					return true;
				}
				// 普通节点事件：只有携带了非空文本数据时才推送给前端
				return sse.data() != null && sse.data().getText() != null && !sse.data().getText().isEmpty();
			})
			// 客户端建立 SSE 连接时触发（前端 new EventSource(url) 成功后）
			.doOnSubscribe(subscription -> log.info("Client subscribed to stream, threadId: {}", request.getThreadId()))
			// 客户端主动断开连接时触发（用户关闭页面、网络中断等）。
			// 需要通知服务层停止对应 threadId 的图执行，释放资源（如取消正在进行的 LLM 调用）。
			.doOnCancel(() -> {
				log.info("Client disconnected from stream, threadId: {}", request.getThreadId());
				if (request.getThreadId() != null) {
					graphService.stopStreamProcessing(request.getThreadId());
				}
			})
			// 流处理过程中发生异常时触发（如 LLM API 调用失败、数据库连接异常等）。
			// 同样需要停止图执行，防止后台线程持续消耗资源。
			.doOnError(e -> {
				log.error("Error occurred during streaming, threadId: {}: ", request.getThreadId(), e);
				if (request.getThreadId() != null) {
					graphService.stopStreamProcessing(request.getThreadId());
				}
			})
			// 流正常完成时触发（StateGraph 所有节点执行完毕，sink 收到 complete 信号）
			.doOnComplete(() -> log.info("Stream completed successfully, threadId: {}", request.getThreadId()));
	}

}
