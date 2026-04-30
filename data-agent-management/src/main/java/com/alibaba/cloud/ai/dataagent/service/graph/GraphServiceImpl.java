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
package com.alibaba.cloud.ai.dataagent.service.graph;

import com.alibaba.cloud.ai.dataagent.service.langfuse.LangfuseService;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.workflow.node.PlannerNode;
import com.alibaba.cloud.ai.dataagent.dto.GraphRequest;
import com.alibaba.cloud.ai.dataagent.service.graph.Context.MultiTurnContextManager;
import com.alibaba.cloud.ai.dataagent.service.graph.Context.StreamContext;
import com.alibaba.cloud.ai.dataagent.vo.GraphNodeResponse;
import com.alibaba.cloud.ai.graph.*;
import com.alibaba.cloud.ai.graph.exception.GraphRunnerException;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import io.opentelemetry.api.trace.Span;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * DataAgent 图编排服务的核心实现类。
 *
 * <h3>在系统中的角色</h3>
 * 本类是整个 DataAgent 工作流的"引擎"，负责将用户的自然语言查询驱动通过一个预定义的有向图
 * （StateGraph）来完成从意图识别、Schema 召回、计划生成、SQL/Python 执行到报告生成的完整流程。
 * 图中的每个节点对应一个独立的 AI 能力（如 Text-to-SQL、代码生成、报告生成等），节点之间通过
 * 共享的 {@link OverAllState} 传递数据。
 *
 * <h3>核心概念说明</h3>
 * <ul>
 *   <li><b>StateGraph（状态图）</b>：Spring AI Alibaba Graph 提供的有向图执行框架。图由多个节点组成，
 *       每个节点是一个独立的处理单元。图编译后生成 {@link CompiledGraph}，可反复执行。</li>
 *   <li><b>OverAllState</b>：图的全局共享状态，以 Map 形式存储。所有节点通过读写同一份状态来协作，
 *       类似于"共享黑板"模式。</li>
 *   <li><b>SSE（Server-Sent Events）</b>：服务端向浏览器单向推送的 HTTP 长连接协议。
 *       本类通过 Reactor 的 {@link Sinks.Many} 向前端实时推送每个图节点的流式输出。</li>
 *   <li><b>Sinks.Many</b>：Reactor 提供的"热源"（Hot Publisher），允许多处代码向同一个数据通道
 *       推送数据，所有订阅者都能收到。类似于一个线程安全的事件总线。</li>
 *   <li><b>Flux&lt;NodeOutput&gt;</b>：Reactor 的异步数据流，表示图节点输出的有序序列。
 *       每当图执行完一个节点，就会向该流发射一个 {@link NodeOutput} 事件。</li>
 *   <li><b>Human-in-the-Loop</b>：人工审核机制。图在执行到 {@code HUMAN_FEEDBACK_NODE} 节点前会自动
 *       暂停（通过 {@code interruptBefore} 配置），等待用户审核 Planner 生成的执行计划后，再通过
 *       {@link #handleHumanFeedback} 恢复执行。</li>
 *   <li><b>Langfuse</b>：LLM 应用的可观测性平台，用于追踪每次图执行的输入输出、延迟和异常。</li>
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>{@link StateGraph} — 由 {@link com.alibaba.cloud.ai.dataagent.config.DataAgentConfiguration}
 *       中通过 DSL 构建并注入，包含所有工作流节点的定义和边</li>
 *   <li>{@link MultiTurnContextManager} — 多轮对话上下文管理器，维护同一会话的历史对话</li>
 *   <li>{@link LangfuseService} — 可观测性服务，负责记录每次图执行的追踪信息</li>
 *   <li>{@link GraphController} — REST 控制器，接收 HTTP 请求并调用本类的方法</li>
 *   <li>{@link StreamContext} — 每个 SSE 连接的上下文对象，封装了 Sink、Disposable、Span 等状态</li>
 * </ul>
 *
 * @see GraphService
 * @see CompiledGraph
 * @see StreamContext
 */
@Slf4j
@Service
public class GraphServiceImpl implements GraphService {

	/** 编译后的状态图实例。StateGraph 在构造时通过 DSL 定义节点和边，compile() 后生成不可变的可执行图 */
	private final CompiledGraph compiledGraph;

	/** 用于异步执行图订阅任务的线程池，避免在请求线程中阻塞等待 Flux 完成 */
	private final ExecutorService executor;

	/**
	 * SSE 会话上下文映射表。Key 为 threadId（每次前端请求的唯一标识），Value 为该会话的流式上下文。
	 * 使用 ConcurrentHashMap 保证多线程并发安全（多个请求可能同时操作此 Map）。
	 */
	private final ConcurrentHashMap<String, StreamContext> streamContextMap = new ConcurrentHashMap<>();

	/** 多轮对话上下文管理器，负责构建和维护同一会话（threadId）的历史对话记录 */
	private final MultiTurnContextManager multiTurnContextManager;

	/** Langfuse 可观测性服务，用于记录 LLM 调用的追踪 span（调用链路、耗时、输入输出） */
	private final LangfuseService langfuseReporter;

	/**
	 * 构造函数：编译状态图并初始化依赖。
	 *
	 * <p>关键步骤是将 {@link StateGraph} 编译为 {@link CompiledGraph}，同时配置中断点。
	 * {@code interruptBefore(HUMAN_FEEDBACK_NODE)} 表示图在执行到人工审核节点之前会自动暂停，
	 * 等待用户通过前端提交审核意见后，再调用 {@link #handleHumanFeedback} 恢复执行。
	 * 这是 "Human-in-the-Loop" 模式的核心实现机制。</p>
	 *
	 * @param stateGraph              由 Spring 容器注入的状态图定义（包含所有节点和边的 DSL 描述）
	 * @param executorService         异步执行线程池
	 * @param multiTurnContextManager 多轮对话上下文管理器
	 * @param langfuseReporter        Langfuse 追踪服务
	 * @throws GraphStateException 如果图定义存在环、孤立节点等结构性问题
	 */
	public GraphServiceImpl(StateGraph stateGraph, ExecutorService executorService,
			MultiTurnContextManager multiTurnContextManager, LangfuseService langfuseReporter)
			throws GraphStateException {
		// 编译图并设置中断点：执行到 HumanFeedbackNode 之前自动暂停，等待人工审核后通过 handleHumanFeedback() 恢复
		this.compiledGraph = stateGraph.compile(CompileConfig.builder().interruptBefore(HUMAN_FEEDBACK_NODE).build());
		this.executor = executorService;
		this.multiTurnContextManager = multiTurnContextManager;
		this.langfuseReporter = langfuseReporter;
	}

	/**
	 * 纯自然语言转 SQL（非流式，阻塞调用）。
	 *
	 * <p>此方法用于简单的 NL2SQL 场景：仅将自然语言翻译为 SQL 并返回，不经过完整的工作流
	 * （不执行报告生成、Python 分析等后续节点）。通过 {@code IS_ONLY_NL2SQL=true} 标记
	 * 控制图内部走短路路径。</p>
	 *
	 * <p>{@code compiledGraph.invoke()} 是同步阻塞调用，会执行图中的所有节点直到结束，
	 * 返回最终的全局状态 {@link OverAllState}。</p>
	 *
	 * @param naturalQuery 用户的自然语言查询，例如"查询最近30天的销售额"
	 * @param agentId      智能体 ID，用于确定使用哪个数据源、Schema 和提示词配置
	 * @return 生成的 SQL 字符串
	 * @throws GraphRunnerException 图执行过程中的运行时异常
	 */
	@Override
	public String nl2sql(String naturalQuery, String agentId) throws GraphRunnerException {
		// invoke() 同步执行图：传入初始状态（IS_ONLY_NL2SQL=true 表示仅走 NL2SQL 路径），
		// 返回最终状态，从中提取 SQL 生成结果
		OverAllState state = compiledGraph
			.invoke(Map.of(IS_ONLY_NL2SQL, true, INPUT_KEY, naturalQuery, AGENT_ID, agentId),
					RunnableConfig.builder().build())
			.orElseThrow();
		return state.value(SQL_GENERATE_OUTPUT, "");
	}

	/**
	 * 流式图处理入口方法：驱动整个 DataAgent 工作流并通过 SSE 向前端实时推送结果。
	 *
	 * <p>这是核心的图执行方法，被 {@link com.alibaba.cloud.ai.dataagent.controller.GraphController}
	 * 调用。它根据请求内容自动判断是"新查询"还是"人工审核后的恢复"，然后启动或恢复图的执行。</p>
	 *
	 * <h4>SSE 推送机制说明</h4>
	 * <p>参数 {@code sink} 是一个 Reactor {@link Sinks.Many}，它是向 SSE 连接写入数据的通道。
	 * 前端通过 EventSource API 建立 SSE 连接后，后端每调用一次 {@code sink.tryEmitNext()}，
	 * 前端就能实时收到一个事件。整个流程是单向推送（服务端→客户端），不需要 WebSocket 的全双工能力。</p>
	 *
	 * @param sink          SSE 数据推送通道，由 Controller 层创建并传入，前端通过 EventSource 订阅
	 * @param graphRequest  图请求对象，包含用户查询、Agent ID、threadId、人工反馈内容等
	 */
	@Override
	public void graphStreamProcess(Sinks.Many<ServerSentEvent<GraphNodeResponse>> sink, GraphRequest graphRequest) {
		// 如果前端未提供 threadId，自动生成一个（每次新的 SSE 连接对应一个唯一的 threadId）
		if (!StringUtils.hasText(graphRequest.getThreadId())) {
			graphRequest.setThreadId(UUID.randomUUID().toString());
		}
		String threadId = graphRequest.getThreadId();
		// 每个 SSE 会话通过 threadId 独立追踪，ConcurrentHashMap 保证线程安全
		StreamContext context = streamContextMap.computeIfAbsent(threadId, k -> new StreamContext());
		// 将 SSE 推送通道绑定到上下文，后续图节点输出时会通过此 sink 推送给前端
		context.setSink(sink);
		// 分支判断：有反馈内容 → 恢复被中断的图（人工审核）；无反馈 → 全新执行
		if (StringUtils.hasText(graphRequest.getHumanFeedbackContent())) {
			handleHumanFeedback(graphRequest);
		}
		else {
			handleNewProcess(graphRequest);
		}
	}

	/**
	 * 停止指定 threadId 的流式处理。
	 *
	 * <p>当客户端主动断开 SSE 连接时（如用户关闭页面），Controller 层会调用此方法来：</p>
	 * <ol>
	 *   <li>丢弃该会话中尚未完成的多轮对话轮次</li>
	 *   <li>结束 Langfuse 追踪 span</li>
	 *   <li>清理所有资源（取消 Flux 订阅、关闭 Sink）</li>
	 * </ol>
	 *
	 * <p>线程安全保证：使用 {@code streamContextMap.remove()} 的原子操作确保只有一个线程能获取到 context，
	 * 避免与 {@link #handleStreamError} 或 {@link #handleStreamComplete} 的并发冲突。</p>
	 *
	 * @param threadId 要停止的 SSE 会话的唯一标识
	 */
	@Override
	public void stopStreamProcessing(String threadId) {
		if (!StringUtils.hasText(threadId)) {
			return;
		}
		log.info("Stopping stream processing for threadId: {}", threadId);
		// 丢弃该会话中正在进行但尚未完成的多轮对话轮次（防止脏数据残留）
		multiTurnContextManager.discardPending(threadId);
		// remove() 原子操作，确保只有一个线程能获取到 context 并执行清理
		StreamContext context = streamContextMap.remove(threadId);
		if (context != null) {
			// 客户端断开，结束 Langfuse span
			if (context.getSpan() != null && context.getSpan().isRecording()) {
				langfuseReporter.endSpanSuccess(context.getSpan(), threadId, context.getCollectedOutput());
			}
			context.cleanup();
			log.info("Cleaned up stream context for threadId: {}", threadId);
		}
	}

	/**
	 * 处理全新的图执行请求（非人工审核恢复场景）。
	 *
	 * <p>此方法启动一个完整的 StateGraph 执行流程。它会：</p>
	 * <ol>
	 *   <li>校验请求参数（threadId、agentId、query）</li>
	 *   <li>启动 Langfuse 追踪 span（记录本次执行的开始时间和请求信息）</li>
	 *   <li>构建多轮对话上下文（将历史对话拼接为字符串注入图的初始状态）</li>
	 *   <li>调用 {@code compiledGraph.stream()} 以流式模式执行图，获得节点输出的 Flux 流</li>
	 *   <li>订阅该 Flux 流，将每个节点的输出通过 SSE 推送给前端</li>
	 * </ol>
	 *
	 * <p>与 {@link #nl2sql} 不同，这里使用 {@code stream()} 而非 {@code invoke()}，意味着图节点的输出
	 * 会以 Reactive 流的形式逐步返回，而非等到全部执行完毕才返回。</p>
	 *
	 * @param graphRequest 图请求对象
	 */
	private void handleNewProcess(GraphRequest graphRequest) {
		String query = graphRequest.getQuery();
		String agentId = graphRequest.getAgentId();
		String threadId = graphRequest.getThreadId();
		boolean nl2sqlOnly = graphRequest.isNl2sqlOnly();
		// 仅在非 NL2SQL-only 模式下且用户开启了人工审核开关时，才启用 Human-in-the-Loop
		// 注意使用 & 而非 && ：此处是有意使用位与运算，因为 humanFeedback 和 nl2sqlOnly 都是布尔字段
		boolean humanReviewEnabled = graphRequest.isHumanFeedback() & !(nl2sqlOnly);
		if (!StringUtils.hasText(threadId) || !StringUtils.hasText(agentId) || !StringUtils.hasText(query)) {
			throw new IllegalArgumentException("Invalid arguments");
		}
		StreamContext context = streamContextMap.get(threadId);
		if (context == null || context.getSink() == null) {
			throw new IllegalStateException("StreamContext not found for threadId: " + threadId);
		}
		// 检查是否已经清理（客户端可能在我们准备启动之前就已经断开连接），如果已清理则不再启动新的流
		if (context.isCleaned()) {
			log.warn("StreamContext already cleaned for threadId: {}, skipping stream start", threadId);
			return;
		}
		// 开始 Langfuse 追踪
		Span span = langfuseReporter.startLLMSpan("graph-stream", graphRequest);
		context.setSpan(span);

		// 构建多轮对话上下文：将历史对话记录格式化为文本，注入到图的初始状态中，使 LLM 能理解之前的对话内容
		String multiTurnContext = multiTurnContextManager.buildContext(threadId);
		// 记录本轮用户查询为新的对话轮次
		multiTurnContextManager.beginTurn(threadId, query);

		// 构建初始状态 Map（工作流的全局共享状态，所有节点通过它传递数据）
		// 各 Key 的含义：
		//   IS_ONLY_NL2SQL     - 是否仅走 NL2SQL 短路路径
		//   INPUT_KEY          - 用户的自然语言查询
		//   AGENT_ID           - 智能体 ID（决定使用哪个数据源和配置）
		//   HUMAN_REVIEW_ENABLED - 是否在 Planner 节点后暂停等待人工审核
		//   MULTI_TURN_CONTEXT - 多轮对话历史上下文
		//   TRACE_THREAD_ID    - 追踪用的线程 ID
		Flux<NodeOutput> nodeOutputFlux = compiledGraph.stream(
				Map.of(IS_ONLY_NL2SQL, nl2sqlOnly, INPUT_KEY, query, AGENT_ID, agentId, HUMAN_REVIEW_ENABLED,
						humanReviewEnabled, MULTI_TURN_CONTEXT, multiTurnContext, TRACE_THREAD_ID, threadId),
				// threadId 用于 StateGraph 框架追踪图的执行状态（支持中断/恢复），对应框架内部的状态检查点机制
				RunnableConfig.builder().threadId(threadId).build());
		subscribeToFlux(context, nodeOutputFlux, graphRequest, agentId, threadId);
	}

	/**
	 * 处理人工审核反馈，从中断点恢复图的执行。
	 *
	 * <h4>Human-in-the-Loop 机制详解</h4>
	 * <p>当图执行到 Planner（计划生成）节点后，如果启用了人工审核，图会在 HumanFeedbackNode 之前自动暂停
	 * （由构造函数中的 {@code interruptBefore(HUMAN_FEEDBACK_NODE)} 配置）。此时前端的 SSE 连接保持，
	 * 等待用户对生成的执行计划进行审核。</p>
	 *
	 * <p>用户审核后，前端发送包含反馈内容的新请求到同一个 SSE 端点。本方法被调用来：</p>
	 * <ol>
	 *   <li>将用户的反馈数据（通过/拒绝 + 反馈内容）更新到图的检查点状态中</li>
	 *   <li>调用 {@code compiledGraph.stream(null, resumeConfig)} 从中断点恢复执行</li>
	 * </ol>
	 *
	 * <p>关键点：传入 {@code null} 作为初始状态表示"恢复模式"（从中断点继续），而非从头开始新的图执行。</p>
	 *
	 * @param graphRequest 图请求对象，必须包含 humanFeedbackContent（反馈内容）和 isRejectedPlan（是否拒绝计划）
	 */
	private void handleHumanFeedback(GraphRequest graphRequest) {
		String agentId = graphRequest.getAgentId();
		String threadId = graphRequest.getThreadId();
		String feedbackContent = graphRequest.getHumanFeedbackContent();
		if (!StringUtils.hasText(threadId) || !StringUtils.hasText(agentId) || !StringUtils.hasText(feedbackContent)) {
			throw new IllegalArgumentException("Invalid arguments");
		}
		StreamContext context = streamContextMap.get(threadId);
		if (context == null || context.getSink() == null) {
			throw new IllegalStateException("StreamContext not found for threadId: " + threadId);
		}
		if (context.isCleaned()) {
			log.warn("StreamContext already cleaned for threadId: {}, skipping stream start", threadId);
			return;
		}
		// 开始 Langfuse 追踪（记录本轮反馈操作）
		Span span = langfuseReporter.startLLMSpan("graph-feedback", graphRequest);
		context.setSpan(span);

		// 构建反馈数据：feedback=true 表示通过计划，false 表示拒绝；feedback_content 是用户的审核意见
		Map<String, Object> feedbackData = Map.of("feedback", !graphRequest.isRejectedPlan(), "feedback_content",
				feedbackContent);
		// 如果用户拒绝了计划，需要回退多轮对话上下文到上一轮（去掉之前记录的本次查询）
		if (graphRequest.isRejectedPlan()) {
			multiTurnContextManager.restartLastTurn(threadId);
		}
		// 将反馈数据和最新的多轮对话上下文合并到图的状态更新中
		Map<String, Object> stateUpdate = new HashMap<>();
		stateUpdate.put(HUMAN_FEEDBACK_DATA, feedbackData);
		stateUpdate.put(MULTI_TURN_CONTEXT, multiTurnContextManager.buildContext(threadId));

		// 基于 threadId 获取图当前的检查点（Checkpoint）配置，用于定位中断点的位置
		RunnableConfig baseConfig = RunnableConfig.builder().threadId(threadId).build();
		RunnableConfig updatedConfig;
		try {
			// updateState() 将反馈数据写入图在该 threadId 下的持久化检查点，
			// 这样恢复执行时，HumanFeedbackNode 节点可以读取到用户的反馈内容
			updatedConfig = compiledGraph.updateState(baseConfig, stateUpdate);
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to update graph state for human feedback", e);
		}
		// 将反馈数据附加到 RunnableConfig 的 metadata 中，供节点通过上下文读取
		RunnableConfig resumeConfig = RunnableConfig.builder(updatedConfig)
			.addMetadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY, feedbackData)
			.build();

		// 传入 null 表示恢复模式（从中断点继续），而非从头开始
		Flux<NodeOutput> nodeOutputFlux = compiledGraph.stream(null, resumeConfig);
		subscribeToFlux(context, nodeOutputFlux, graphRequest, agentId, threadId);
	}

	/**
	 * 订阅图的 Flux 输出流，并原子性地将 Disposable 绑定到上下文。
	 *
	 * <p>此方法将图的节点输出流（{@link Flux}）的订阅操作提交到异步线程池执行，
	 * 避免在请求线程中阻塞。订阅后会持续接收图节点的流式输出，直到图执行完成或发生错误。</p>
	 *
	 * <h4>线程安全设计</h4>
	 * <p>使用 {@code synchronized(context)} 确保 Disposable 的设置与 context 的清理状态检查是原子操作，
	 * 防止以下竞态条件：在订阅成功后、设置 Disposable 之前，另一个线程调用了 {@link #stopStreamProcessing}
	 * 导致 context 被清理，但 Disposable 未被取消，造成资源泄漏。</p>
	 *
	 * @param context        当前会话的流式上下文
	 * @param nodeOutputFlux 图节点输出的 Reactive 流，每执行完一个节点就发射一个事件
	 * @param graphRequest   原始图请求
	 * @param agentId        智能体 ID
	 * @param threadId       会话线程 ID
	 */
	private void subscribeToFlux(StreamContext context, Flux<NodeOutput> nodeOutputFlux, GraphRequest graphRequest,
			String agentId, String threadId) {
		// 在异步线程池中执行订阅，避免阻塞请求线程（图执行可能持续数十秒甚至更长时间）
		CompletableFuture.runAsync(() -> {
			// 在订阅之前检查上下文是否仍然有效（客户端可能在我们排队期间已经断开）
			if (context.isCleaned()) {
				log.debug("StreamContext cleaned before subscription for threadId: {}", threadId);
				return;
			}
			// subscribe() 返回 Disposable，可用于取消订阅（停止图执行）
			Disposable disposable = nodeOutputFlux.subscribe(output -> handleNodeOutput(graphRequest, output),
					error -> handleStreamError(agentId, threadId, error),
					() -> handleStreamComplete(agentId, threadId));
			// 原子性地设置 Disposable，如果已经清理则立即释放
			synchronized (context) {
				if (context.isCleaned()) {
					// 如果已经清理，立即释放刚创建的 Disposable，防止资源泄漏
					if (disposable != null && !disposable.isDisposed()) {
						disposable.dispose();
					}
				}
				else {
					// 只有在未清理的情况下才设置 Disposable，以便后续 stopStreamProcessing 可以取消订阅
					context.setDisposable(disposable);
				}
			}
		}, executor);
	}

	/**
	 * 处理图执行过程中的流式错误。
	 *
	 * <p>当图执行过程中某个节点抛出异常时，Flux 的 error 回调会触发此方法。
	 * 它负责：向前端发送错误事件（SSE event type = "error"）、结束 Langfuse 追踪 span、清理资源。</p>
	 *
	 * <p>线程安全保证：使用 {@code streamContextMap.remove()} 操作确保只有一个线程能获取到 context，
	 * 避免与 {@link #handleStreamComplete} 或 {@link #stopStreamProcessing} 的并发冲突。</p>
	 *
	 * @param agentId  智能体 ID，用于构造错误响应
	 * @param threadId 会话线程 ID
	 * @param error    错误信息
	 */
	private void handleStreamError(String agentId, String threadId, Throwable error) {
		log.error("Error in stream processing for threadId: {}: ", threadId, error);
		// remove() 保证只有一个线程能获取到 context（原子操作），防止并发清理
		StreamContext context = streamContextMap.remove(threadId);
		if (context != null && !context.isCleaned()) {
			// 结束 Langfuse span（标记为失败）
			if (context.getSpan() != null) {
				langfuseReporter.endSpanError(context.getSpan(), threadId,
						error instanceof Exception ? (Exception) error : new RuntimeException(error));
			}
			// currentSubscriberCount() > 0 检查前端是否仍在监听 SSE 连接
			if (context.getSink() != null && context.getSink().currentSubscriberCount() > 0) {
				// 向前端发送错误事件，前端通过 event type "error" 识别并展示错误信息
				context.getSink()
					.tryEmitNext(ServerSentEvent
						.builder(GraphNodeResponse.error(agentId, threadId,
								"Error in stream processing: " + error.getMessage()))
						.event(STREAM_EVENT_ERROR)
						.build());
				// 关闭 SSE 连接（发送完成信号）
				context.getSink().tryEmitComplete();
			}
			// 清理资源（cleanup 内部通过 AtomicBoolean.compareAndSet 保证只执行一次）
			context.cleanup();
		}
	}

	/**
	 * 处理图执行完成（成功结束）。
	 *
	 * <p>当 StateGraph 中的所有节点都执行完毕后，Flux 的 onComplete 回调会触发此方法。
	 * 它负责：标记多轮对话轮次完成、向前端发送完成事件（SSE event type = "complete"）、
	 * 结束 Langfuse 追踪 span、清理资源。</p>
	 *
	 * <p>线程安全保证：与 {@link #handleStreamError} 相同，使用 {@code streamContextMap.remove()}
	 * 确保只有一个线程能获取到 context，两者互斥执行。</p>
	 *
	 * @param agentId  智能体 ID，用于构造完成响应
	 * @param threadId 会话线程 ID
	 */
	private void handleStreamComplete(String agentId, String threadId) {
		log.info("Stream processing completed successfully for threadId: {}", threadId);
		// 标记当前多轮对话轮次已完成，将本轮的查询和结果持久化到对话历史中
		multiTurnContextManager.finishTurn(threadId);
		// remove() 保证只有一个线程能获取到 context（原子操作），与 handleStreamError 互斥
		StreamContext context = streamContextMap.remove(threadId);
		if (context != null && !context.isCleaned()) {
			// 结束 Langfuse span（标记为成功），并上报收集到的完整输出内容
			if (context.getSpan() != null) {
				langfuseReporter.endSpanSuccess(context.getSpan(), threadId, context.getCollectedOutput());
			}
			// 向前端发送完成事件，前端收到后关闭 EventSource 连接
			if (context.getSink() != null && context.getSink().currentSubscriberCount() > 0) {
				context.getSink()
					.tryEmitNext(ServerSentEvent.builder(GraphNodeResponse.complete(agentId, threadId))
						.event(STREAM_EVENT_COMPLETE)
						.build());
				// 关闭 SSE 连接（发送完成信号）
				context.getSink().tryEmitComplete();
			}
			context.cleanup();
		}
	}

	/**
	 * 处理图节点输出的分发。
	 *
	 * <p>{@link NodeOutput} 是 Spring AI Alibaba Graph 框架定义的节点输出接口，
	 * 目前有两种实现：</p>
	 * <ul>
	 *   <li>{@link StreamingOutput} — 流式输出，包含节点名称和一个文本 chunk（LLM 生成的文本片段）</li>
	 *   <li>其他类型 — 非流式输出（如最终状态快照），本方法暂不处理</li>
	 * </ul>
	 *
	 * @param request 图请求对象（用于获取 threadId 和 agentId）
	 * @param output  图节点的输出事件
	 */
	private void handleNodeOutput(GraphRequest request, NodeOutput output) {
		log.debug("Received output: {}", output.getClass().getSimpleName());
		// 使用 Java 21 的模式匹配 instanceof，仅处理流式输出（LLM 逐 token 生成的文本片段）
		if (output instanceof StreamingOutput streamingOutput) {
			handleStreamNodeOutput(request, streamingOutput);
		}
	}

	/**
	 * 处理单个流式文本 chunk 的输出。
	 *
	 * <p>这是 SSE 推送的核心处理方法。每当 LLM 或图节点产生一个文本片段（chunk），此方法被调用。
	 * 它需要处理一个关键问题：LLM 生成的文本流中包含了"类型标记"（如 {@code "$$$sql"}、{@code "$$$python"}），
	 * 用于标识后续文本的内容类型。这些标记由后端节点插入，前端不需要展示，因此需要过滤掉。</p>
	 *
	 * <h4>文本类型标记协议说明</h4>
	 * <p>图节点在输出流中通过特殊的分隔符标记来告知前端后续文本的类型：</p>
	 * <ul>
	 *   <li>{@code "$$$sql"} — 标记后续文本为 SQL 代码</li>
	 *   <li>{@code "$$$python"} — 标记后续文本为 Python 代码</li>
	 *   <li>{@code "$$$json"} — 标记后续文本为 JSON 数据</li>
	 *   <li>{@code "$$$markdown-report"} — 标记后续文本为 Markdown 报告</li>
	 *   <li>{@code "$$$result_set"} — 标记后续文本为查询结果集</li>
	 * </ul>
	 * <p>开始标记之后、结束标记（{@code "$$$"}）之前的所有 chunk 都属于该类型。
	 * 结束标记之后恢复为普通文本类型。</p>
	 *
	 * @param request 图请求对象
	 * @param output  流式输出，包含 node（节点名称）和 chunk（文本片段）
	 */
	private void handleStreamNodeOutput(GraphRequest request, StreamingOutput output) {
		String threadId = request.getThreadId();
		StreamContext context = streamContextMap.get(threadId);
		// 检查是否已经停止处理（流可能在处理过程中被停止）
		if (context == null || context.getSink() == null) {
			log.debug("Stream processing already stopped for threadId: {}, skipping output", threadId);
			return;
		}
		// node 是产生此输出的图节点名称（如 "SqlGenerateNode"、"PlannerNode"）
		String node = output.node();
		// chunk 是 LLM 生成的文本片段（通常是一个 token 或几个 token 的文本）
		String chunk = output.chunk();
		log.debug("Received Stream output: {}", chunk);

		if (chunk == null || chunk.isEmpty()) {
			return;
		}

		// ---- 文本类型状态机：解析类型标记，跟踪当前输出的文本类型 ----
		// 类似于一个简单的协议解析器：遇到开始标记则切换类型，遇到结束标记则恢复为 TEXT
		TextType originType = context.getTextType();
		TextType textType;
		boolean isTypeSign = false;
		if (originType == null) {
			// 首次输出，尝试匹配开始标记确定文本类型
			textType = TextType.getTypeByStratSign(chunk);
			if (textType != TextType.TEXT) {
				isTypeSign = true; // 当前 chunk 是类型标记，不推送给前端
			}
			context.setTextType(textType);
		}
		else {
			// 已有类型，检查是否为结束标记
			textType = TextType.getType(originType, chunk);
			if (textType != originType) {
				isTypeSign = true; // 类型发生变化（遇到结束标记），当前 chunk 不推送
			}
			context.setTextType(textType);
		}

		// 文本类型标记符号不返回给前端，仅普通内容文本才推送
		if (!isTypeSign) {
			// 收集输出内容用于 Langfuse 追踪上报（完整的 LLM 输出记录）
			context.appendOutput(chunk);
			// 如果输出来自 PlannerNode，额外追加到多轮对话上下文中（记录 Planner 的计划供后续轮次参考）
			if (PlannerNode.class.getSimpleName().equals(node)) {
				multiTurnContextManager.appendPlannerChunk(threadId, chunk);
			}
			GraphNodeResponse response = GraphNodeResponse.builder()
				.agentId(request.getAgentId())
				.threadId(threadId)
				.nodeName(node)
				.text(chunk)
				.textType(textType)
				.build();
			// tryEmitNext 是非阻塞的尝试发射方法，不会因为下游背压而阻塞当前线程
			// 检查发送是否成功，如果失败说明客户端已断开或 Sink 已被取消
			Sinks.EmitResult result = context.getSink().tryEmitNext(ServerSentEvent.builder(response).build());
			if (result.isFailure()) {
				log.warn("Failed to emit data to sink for threadId: {}, result: {}. Stopping stream processing.",
						threadId, result);
				// 如果发送失败（客户端已断开），主动停止处理并清理资源
				stopStreamProcessing(threadId);
			}
		}
	}

}
