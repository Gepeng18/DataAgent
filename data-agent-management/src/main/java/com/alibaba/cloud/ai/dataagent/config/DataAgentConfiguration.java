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
package com.alibaba.cloud.ai.dataagent.config;

import com.alibaba.cloud.ai.dataagent.properties.CodeExecutorProperties;
import com.alibaba.cloud.ai.dataagent.properties.DataAgentProperties;
import com.alibaba.cloud.ai.dataagent.properties.FileStorageProperties;
import com.alibaba.cloud.ai.dataagent.service.vectorstore.SimpleVectorStoreInitialization;
import com.alibaba.cloud.ai.dataagent.splitter.SentenceSplitter;
import com.alibaba.cloud.ai.transformer.splitter.RecursiveCharacterTextSplitter;
import com.alibaba.cloud.ai.dataagent.splitter.SemanticTextSplitter;
import com.alibaba.cloud.ai.dataagent.splitter.ParagraphTextSplitter;
import com.alibaba.cloud.ai.dataagent.util.McpServerToolUtil;
import com.alibaba.cloud.ai.dataagent.util.NodeBeanUtil;
import com.alibaba.cloud.ai.dataagent.service.aimodelconfig.AiModelRegistry;
import com.alibaba.cloud.ai.dataagent.strategy.EnhancedTokenCountBatchingStrategy;
import com.alibaba.cloud.ai.dataagent.workflow.dispatcher.*;
import com.alibaba.cloud.ai.dataagent.workflow.node.*;
import com.alibaba.cloud.ai.graph.GraphRepresentation;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.KeyStrategyFactory;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.knuddels.jtokkit.api.EncodingType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.embedding.BatchingStrategy;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.resolution.DelegatingToolCallbackResolver;
import org.springframework.ai.tool.resolution.SpringBeanToolCallbackResolver;
import org.springframework.ai.tool.resolution.StaticToolCallbackResolver;
import org.springframework.ai.tool.resolution.ToolCallbackResolver;
import org.springframework.ai.transformer.splitter.TextSplitter;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.SimpleVectorStore;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.START;
import static com.alibaba.cloud.ai.graph.action.AsyncEdgeAction.edge_async;

/**
 * DataAgent 核心自动配置类 -- 整个系统的"装配车间"。
 *
 * <h3>在系统中的角色</h3>
 * 本类是 Spring Boot 自动配置的入口之一，负责将 DataAgent 的各个核心组件组装到 Spring 容器中。
 * 它是最关键的配置类，因为 {@link #nl2sqlGraph} 方法构建了整个系统的心脏——一个由 16 个节点组成的
 * StateGraph（有向图状态机），用于驱动"自然语言 → 数据分析报告"的完整工作流。
 *
 * <h3>涉及的核心概念</h3>
 * <ul>
 *   <li><b>StateGraph（有向图状态机）</b>：来自 spring-ai-alibaba-graph 框架，类似于 LangGraph 的设计思想。
 *       图中的每个节点（Node）是一个处理步骤，边（Edge）定义了节点之间的跳转逻辑。
 *       节点之间通过共享的 {@code OverAllState} 状态对象传递数据，类似一个全局的黑板模式（Blackboard Pattern）。</li>
 *   <li><b>KeyStrategy（状态键策略）</b>：定义了当多个节点向同一个 state key 写入数据时的合并策略。
 *       {@code REPLACE} 表示后写入的值覆盖先前的值。本项目所有 key 都使用 REPLACE，
 *       因为每个 key 只由一个节点负责写入，不存在并发写入同一 key 的情况。</li>
 *   <li><b>NodeAction / EdgeAction</b>：NodeAction 是节点的业务逻辑接口，接收当前状态、处理后返回更新；
 *       EdgeAction 是条件边的路由逻辑接口，根据当前状态决定下一个跳转目标。</li>
 *   <li><b>Dispatcher（分发器）</b>：本项目中 EdgeAction 的具体实现，每个分发器负责根据节点输出结果
 *       决定下一个要执行的节点。例如 IntentRecognitionDispatcher 判断用户意图是否为闲聊，
 *       如果是则直接结束流程，否则进入后续的数据分析流程。</li>
 *   <li><b>VectorStore（向量存储）</b>：用于 RAG（Retrieval-Augmented Generation，检索增强生成）的向量数据库，
 *       将文本转化为高维向量后存储，支持语义相似度检索。默认使用内存实现 SimpleVectorStore。</li>
 *   <li><b>TextSplitter（文本分块器）</b>：将长文档切分为多个小块（chunk），以便 Embedding 模型处理。
 *       本项目提供了 token、recursive、sentence、semantic、paragraph 五种分块策略。</li>
 *   <li><b>EmbeddingModel（嵌入模型）</b>：将文本转化为固定维度的浮点数向量（embedding），
 *       用于语义相似度计算。本配置通过动态代理实现了运行时热切换模型的能力。</li>
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>{@link com.alibaba.cloud.ai.dataagent.service.graph.GraphServiceImpl} -- 消费本类构建的 StateGraph，
 *       提供 SSE 流式接口驱动图执行</li>
 *   <li>{@link com.alibaba.cloud.ai.dataagent.workflow.node} 包下的各种 Node -- 图中的节点实现</li>
 *   <li>{@link com.alibaba.cloud.ai.dataagent.workflow.dispatcher} 包下的各种 Dispatcher -- 图中的条件边实现</li>
 *   <li>{@link com.alibaba.cloud.ai.dataagent.properties.DataAgentProperties} -- 外部化配置属性</li>
 * </ul>
 *
 * @author vlsmb
 * @since 2025/9/28
 * @see StateGraph
 * @see com.alibaba.cloud.ai.graph.OverAllState
 * @see com.alibaba.cloud.ai.graph.action.NodeAction
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
@Configuration
@EnableAsync
@EnableConfigurationProperties({ CodeExecutorProperties.class, DataAgentProperties.class, FileStorageProperties.class })
public class DataAgentConfiguration implements DisposableBean {

	/** 专用线程池，用于数据库操作的并行处理（如批量 Schema 信息查询等），在 {@link #destroy()} 中优雅关闭 */
	private ExecutorService dbOperationExecutor;

	/**
	 * 配置同步 HTTP 客户端（RestClient）的超时参数。
	 *
	 * <p>该 RestClient 主要用于调用 LLM 的非流式接口（BLOCK 模式），以及与外部服务通信。
	 * 默认超时 600 秒，因为 LLM 生成复杂 SQL/代码时响应可能较慢。
	 * 使用 reactor 类型的 requestFactory 以获得非阻塞 IO 能力。</p>
	 *
	 * @param connectTimeout 连接超时（秒），默认 600
	 * @param readTimeout    读取超时（秒），默认 600
	 * @return RestClientCustomizer 用于定制 RestClient.Builder
	 */
	@Bean
	@ConditionalOnMissingBean(RestClientCustomizer.class)
	public RestClientCustomizer restClientCustomizer(@Value("${rest.connect.timeout:600}") long connectTimeout,
			@Value("${rest.read.timeout:600}") long readTimeout) {
		return restClientBuilder -> restClientBuilder
			.requestFactory(ClientHttpRequestFactoryBuilder.reactor().withCustomizer(factory -> {
				factory.setConnectTimeout(Duration.ofSeconds(connectTimeout));
				factory.setReadTimeout(Duration.ofSeconds(readTimeout));
			}).build());
	}

	/**
	 * 配置异步 HTTP 客户端（WebClient）的超时参数。
	 *
	 * <p>WebClient 基于 Reactor Netty，主要用于调用 LLM 的流式接口（STREAM 模式），
	 * 如 OpenAI 兼容 API 的 SSE（Server-Sent Events）流式响应。通过 SSE，
	 * 系统可以将 LLM 逐步生成的 SQL、分析文本等实时推送到前端。</p>
	 *
	 * @param responseTimeout 响应超时（秒），默认 600
	 * @return WebClient.Builder 实例
	 */
	@Bean
	@ConditionalOnMissingBean(WebClient.Builder.class)
	public WebClient.Builder webClientBuilder(@Value("${webclient.response.timeout:600}") long responseTimeout) {

		return WebClient.builder()
			.clientConnector(new ReactorClientHttpConnector(
					HttpClient.create().responseTimeout(Duration.ofSeconds(responseTimeout))));
	}

	/**
	 * 构建核心工作流有向图 -- 整个 DataAgent 系统的心脏。
	 *
	 * <h3>什么是 StateGraph？</h3>
	 * <p>StateGraph 是 spring-ai-alibaba-graph 框架提供的核心抽象，本质上是一个<b>有向图状态机</b>，
	 * 设计灵感来自 LangGraph。它的运行机制是：</p>
	 * <ol>
	 *   <li>图有一个全局共享的 {@code OverAllState} 状态对象（类似"黑板"）</li>
	 *   <li>节点（NodeAction）从状态中读取输入，执行业务逻辑后，将结果写回状态</li>
	 *   <li>条件边（EdgeAction / Dispatcher）根据当前状态决定下一个要执行的节点</li>
	 *   <li>框架按照拓扑顺序依次执行节点，直到到达 END 终止节点</li>
	 * </ol>
	 *
	 * <h3>本图的完整工作流拓扑</h3>
	 * <pre>
	 *   START
	 *     → IntentRecognition（意图识别：判断用户是在闲聊还是需要数据分析）
	 *       → [闲聊] → END
	 *       → [数据分析] → EvidenceRecall（RAG检索：从知识库中召回相关证据文档）
	 *         → QueryEnhance（查询改写：优化用户原始查询以提升检索质量）
	 *           → [不可处理] → END
	 *           → [可处理] → SchemaRecall（Schema召回：检索相关表和列的元数据）
	 *             → TableRelation（表关系推断：分析表之间的 JOIN 关系）
	 *               → [重试/失败] → TableRelation 或 END
	 *               → [成功] → FeasibilityAssessment（可行性评估：判断是否能回答用户问题）
	 *                 → [不可行] → END
	 *                 → [可行] → Planner（计划生成：LLM 生成多步骤执行计划）
	 *                   → PlanExecutor（计划执行器：逐步执行计划中的每个步骤）
	 *                     ├→ [验证失败] → Planner（重新生成计划，最多重试2次）
	 *                     ├→ [人工审核] → HumanFeedback（人工审核节点，支持中断等待）
	 *                     ├→ [SQL步骤] → SqlGenerate → SemanticConsistency → SqlExecute → PlanExecutor
	 *                     ├→ [Python步骤] → PythonGenerate → PythonExecute → PythonAnalyze → PlanExecutor
	 *                     └→ [报告步骤] → ReportGenerator → END
	 * </pre>
	 *
	 * <h3>KeyStrategyFactory 说明</h3>
	 * <p>StateGraph 使用 KeyStrategyFactory 定义状态中每个 key 的合并策略。
	 * {@code KeyStrategy.REPLACE} 表示新值覆盖旧值（本项目全部使用此策略，因为每个 key 只有一个节点写入）。
	 * 另一种可选策略是 {@code APPEND}（追加），用于多个节点向同一个 key 添加数据的场景。</p>
	 *
	 * @param nodeBeanUtil            节点工具类，负责将 Spring Bean 包装为 NodeAction/EdgeAction。
	 *                                {@code getNodeBeanAsync} 会将同步 NodeAction 包装为异步执行，
	 *                                使节点在独立线程中运行，避免阻塞图的调度线程。
	 * @param codeExecutorProperties  Python 代码执行器配置（Docker/Local/AI模拟模式等）
	 * @return 构建完成的 StateGraph 实例，后续由 {@code GraphServiceImpl} 编译并驱动执行
	 * @throws GraphStateException 图状态异常（如重复节点名、边指向不存在的节点等）
	 */
	@Bean
	public StateGraph nl2sqlGraph(NodeBeanUtil nodeBeanUtil, CodeExecutorProperties codeExecutorProperties)
			throws GraphStateException {

		// 定义状态键的合并策略工厂。每次图编译时会调用此工厂生成一份 key → strategy 的映射表。
		// 所有 key 统一使用 REPLACE 策略，即后写入的值覆盖先前的值。
		KeyStrategyFactory keyStrategyFactory = () -> {
			HashMap<String, KeyStrategy> keyStrategyHashMap = new HashMap<>();

			// ===== 基础输入 =====
			keyStrategyHashMap.put(INPUT_KEY, KeyStrategy.REPLACE);          // 用户原始输入的自然语言问题
			keyStrategyHashMap.put(AGENT_ID, KeyStrategy.REPLACE);          // 当前 DataAgent 实例 ID，用于隔离不同 Agent 的配置
			keyStrategyHashMap.put(MULTI_TURN_CONTEXT, KeyStrategy.REPLACE); // 多轮对话上下文，由 MultiTurnContextManager 维护

			// ===== 意图识别阶段 =====
			keyStrategyHashMap.put(INTENT_RECOGNITION_NODE_OUTPUT, KeyStrategy.REPLACE); // 意图识别结果（闲聊/数据分析）

			// ===== 查询增强阶段 =====
			keyStrategyHashMap.put(QUERY_ENHANCE_NODE_OUTPUT, KeyStrategy.REPLACE);      // 改写后的查询

			// ===== Schema 构建阶段 =====
			keyStrategyHashMap.put(GENEGRATED_SEMANTIC_MODEL_PROMPT, KeyStrategy.REPLACE); // 生成的语义模型 Prompt（描述数据库结构）

			// ===== RAG 证据召回阶段 =====
			keyStrategyHashMap.put(EVIDENCE, KeyStrategy.REPLACE);                        // 从向量库中召回的相关知识文档

			// ===== Schema 召回阶段（检索与用户问题相关的表和列的元数据） =====
			keyStrategyHashMap.put(TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT, KeyStrategy.REPLACE);   // 召回的相关表文档
			keyStrategyHashMap.put(COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT, KeyStrategy.REPLACE); // 召回的相关列文档

			// ===== 表关系推断阶段（分析表之间的外键/JOIN 关系） =====
			keyStrategyHashMap.put(TABLE_RELATION_OUTPUT, KeyStrategy.REPLACE);          // 推断出的表关系信息
			keyStrategyHashMap.put(TABLE_RELATION_EXCEPTION_OUTPUT, KeyStrategy.REPLACE); // 推断失败时的异常信息
			keyStrategyHashMap.put(TABLE_RELATION_RETRY_COUNT, KeyStrategy.REPLACE);     // 表关系推断重试计数
			keyStrategyHashMap.put(DB_DIALECT_TYPE, KeyStrategy.REPLACE);                // 数据库方言类型（MySQL/PostgreSQL 等）

			// ===== 可行性评估阶段 =====
			keyStrategyHashMap.put(FEASIBILITY_ASSESSMENT_NODE_OUTPUT, KeyStrategy.REPLACE); // 可行性评估结果

			// ===== SQL 生成阶段 =====
			keyStrategyHashMap.put(SQL_GENERATE_SCHEMA_MISSING_ADVICE, KeyStrategy.REPLACE); // Schema 缺失时的建议
			keyStrategyHashMap.put(SQL_GENERATE_OUTPUT, KeyStrategy.REPLACE);                // LLM 生成的 SQL 语句
			keyStrategyHashMap.put(SQL_GENERATE_COUNT, KeyStrategy.REPLACE);                 // SQL 生成/重试次数
			keyStrategyHashMap.put(SQL_REGENERATE_REASON, KeyStrategy.REPLACE);              // SQL 重新生成的原因（如语义校验失败）

			// ===== 语义一致性校验阶段（验证生成的 SQL 是否符合语义要求） =====
			keyStrategyHashMap.put(SEMANTIC_CONSISTENCY_NODE_OUTPUT, KeyStrategy.REPLACE);

			// ===== 计划生成阶段 =====
			keyStrategyHashMap.put(PLANNER_NODE_OUTPUT, KeyStrategy.REPLACE); // LLM 生成的多步骤执行计划（JSON 格式）

			// ===== 计划执行阶段 =====
			keyStrategyHashMap.put(PLAN_CURRENT_STEP, KeyStrategy.REPLACE);      // 当前正在执行的计划步骤编号
			keyStrategyHashMap.put(PLAN_NEXT_NODE, KeyStrategy.REPLACE);         // 下一个要跳转的节点名称（由 PlanExecutor 设定）
			keyStrategyHashMap.put(PLAN_VALIDATION_STATUS, KeyStrategy.REPLACE);  // 计划校验是否通过
			keyStrategyHashMap.put(PLAN_VALIDATION_ERROR, KeyStrategy.REPLACE);   // 计划校验失败的错误信息
			keyStrategyHashMap.put(PLAN_REPAIR_COUNT, KeyStrategy.REPLACE);      // 计划修复重试次数

			// ===== SQL 执行阶段 =====
			keyStrategyHashMap.put(SQL_EXECUTE_NODE_OUTPUT, KeyStrategy.REPLACE); // SQL 执行结果（数据行或错误信息）

			// ===== Python 代码执行阶段 =====
			keyStrategyHashMap.put(SQL_RESULT_LIST_MEMORY, KeyStrategy.REPLACE);       // SQL 查询结果的累积记忆（供 Python 代码使用）
			keyStrategyHashMap.put(PYTHON_IS_SUCCESS, KeyStrategy.REPLACE);            // Python 代码执行是否成功
			keyStrategyHashMap.put(PYTHON_TRIES_COUNT, KeyStrategy.REPLACE);           // Python 代码重试次数
			keyStrategyHashMap.put(PYTHON_FALLBACK_MODE, KeyStrategy.REPLACE);         // 是否进入降级模式（超过最大重试次数后跳过 Python 执行）
			keyStrategyHashMap.put(PYTHON_EXECUTE_NODE_OUTPUT, KeyStrategy.REPLACE);   // Python 代码执行输出
			keyStrategyHashMap.put(PYTHON_GENERATE_NODE_OUTPUT, KeyStrategy.REPLACE);  // LLM 生成的 Python 代码
			keyStrategyHashMap.put(PYTHON_ANALYSIS_NODE_OUTPUT, KeyStrategy.REPLACE);  // Python 代码分析结果

			// ===== NL2SQL 模式标记 =====
			keyStrategyHashMap.put(IS_ONLY_NL2SQL, KeyStrategy.REPLACE); // 是否仅执行 NL2SQL（跳过 Python 分析和报告生成）

			// ===== 人工审核（Human-in-the-loop） =====
			keyStrategyHashMap.put(HUMAN_REVIEW_ENABLED, KeyStrategy.REPLACE); // 是否启用人工审核
			keyStrategyHashMap.put(HUMAN_FEEDBACK_DATA, KeyStrategy.REPLACE);  // 人工审核的反馈数据（通过/拒绝/修改意见）

			// ===== 可观测性 =====
			keyStrategyHashMap.put(TRACE_THREAD_ID, KeyStrategy.REPLACE); // Langfuse 追踪的线程 ID，用于 token 消耗累计

			// ===== 最终输出 =====
			keyStrategyHashMap.put(RESULT, KeyStrategy.REPLACE); // 工作流的最终分析结果（报告内容）
			return keyStrategyHashMap;
		};

		// ===== 注册 16 个工作流节点 =====
		// 节点注册顺序不影响执行顺序，实际执行由下方边（edge）定义的拓扑决定。
		// NodeBeanUtil.getNodeBeanAsync() 将 Spring Bean 中的 NodeAction 方法包装为异步执行，
		// 使每个节点在独立线程中运行，避免阻塞图的调度线程。
		StateGraph stateGraph = new StateGraph(NL2SQL_GRAPH_NAME, keyStrategyFactory)
			.addNode(INTENT_RECOGNITION_NODE, nodeBeanUtil.getNodeBeanAsync(IntentRecognitionNode.class))
			.addNode(EVIDENCE_RECALL_NODE, nodeBeanUtil.getNodeBeanAsync(EvidenceRecallNode.class))
			.addNode(QUERY_ENHANCE_NODE, nodeBeanUtil.getNodeBeanAsync(QueryEnhanceNode.class))
			.addNode(SCHEMA_RECALL_NODE, nodeBeanUtil.getNodeBeanAsync(SchemaRecallNode.class))
			.addNode(TABLE_RELATION_NODE, nodeBeanUtil.getNodeBeanAsync(TableRelationNode.class))
			.addNode(FEASIBILITY_ASSESSMENT_NODE, nodeBeanUtil.getNodeBeanAsync(FeasibilityAssessmentNode.class))
			.addNode(SQL_GENERATE_NODE, nodeBeanUtil.getNodeBeanAsync(SqlGenerateNode.class))
			.addNode(PLANNER_NODE, nodeBeanUtil.getNodeBeanAsync(PlannerNode.class))
			.addNode(PLAN_EXECUTOR_NODE, nodeBeanUtil.getNodeBeanAsync(PlanExecutorNode.class))
			.addNode(SQL_EXECUTE_NODE, nodeBeanUtil.getNodeBeanAsync(SqlExecuteNode.class))
			.addNode(PYTHON_GENERATE_NODE, nodeBeanUtil.getNodeBeanAsync(PythonGenerateNode.class))
			.addNode(PYTHON_EXECUTE_NODE, nodeBeanUtil.getNodeBeanAsync(PythonExecuteNode.class))
			.addNode(PYTHON_ANALYZE_NODE, nodeBeanUtil.getNodeBeanAsync(PythonAnalyzeNode.class))
			.addNode(REPORT_GENERATOR_NODE, nodeBeanUtil.getNodeBeanAsync(ReportGeneratorNode.class))
			.addNode(SEMANTIC_CONSISTENCY_NODE, nodeBeanUtil.getNodeBeanAsync(SemanticConsistencyNode.class))
			.addNode(HUMAN_FEEDBACK_NODE, nodeBeanUtil.getNodeBeanAsync(HumanFeedbackNode.class));

		// ===== 定义图的边（节点跳转逻辑） =====
		// addEdge(A, B)                          -- 无条件边：节点 A 执行完毕后一定跳转到节点 B
		// addConditionalEdges(A, dispatcher, Map) -- 条件边：dispatcher 根据 A 的输出决定下一个节点，
		//                                          Map 定义了 dispatcher 返回值到目标节点的映射关系。
		// edge_async()                           -- 将同步 EdgeAction 包装为异步执行。
		stateGraph.addEdge(START, INTENT_RECOGNITION_NODE)  // 入口：START 伪节点 → 意图识别
			// 意图识别后的分支：闲聊→END，数据分析→证据召回
			.addConditionalEdges(INTENT_RECOGNITION_NODE, edge_async(new IntentRecognitionDispatcher()),
					Map.of(EVIDENCE_RECALL_NODE, EVIDENCE_RECALL_NODE, END, END))
			// 证据召回 → 查询增强（无条件，顺序执行）
			.addEdge(EVIDENCE_RECALL_NODE, QUERY_ENHANCE_NODE)
			// 查询增强后的分支：可处理→Schema召回，不可处理→END
			.addConditionalEdges(QUERY_ENHANCE_NODE, edge_async(new QueryEnhanceDispatcher()),
					Map.of(SCHEMA_RECALL_NODE, SCHEMA_RECALL_NODE, END, END))
			// Schema召回后的分支：有相关表→表关系推断，无相关表→END
			.addConditionalEdges(SCHEMA_RECALL_NODE, edge_async(new SchemaRecallDispatcher()),
					Map.of(TABLE_RELATION_NODE, TABLE_RELATION_NODE, END, END))

			// 表关系推断后的分支：成功→可行性评估，失败→END，异常→自身重试
			.addConditionalEdges(TABLE_RELATION_NODE, edge_async(new TableRelationDispatcher()),
					Map.of(FEASIBILITY_ASSESSMENT_NODE, FEASIBILITY_ASSESSMENT_NODE, END, END, TABLE_RELATION_NODE,
							TABLE_RELATION_NODE)) // retry: 推断失败时路由回自身重试
			// 可行性评估后的分支：可行→计划生成，不可行→END
			.addConditionalEdges(FEASIBILITY_ASSESSMENT_NODE, edge_async(new FeasibilityAssessmentDispatcher()),
					Map.of(PLANNER_NODE, PLANNER_NODE, END, END))

			// ===== 计划生成与执行阶段 =====
			// Planner 生成执行计划后，无条件交给 PlanExecutor 进行校验和逐步执行
			.addEdge(PLANNER_NODE, PLAN_EXECUTOR_NODE)

			// ===== Python 代码执行管道 =====
			// PythonGenerate（LLM 生成 Python 分析代码）→ PythonExecute（在 Docker/本地执行代码）
			.addEdge(PYTHON_GENERATE_NODE, PYTHON_EXECUTE_NODE)
			// Python 执行后的分支：成功→Python分析，失败重试→重新生成，超过重试→END
			.addConditionalEdges(PYTHON_EXECUTE_NODE, edge_async(new PythonExecutorDispatcher(codeExecutorProperties)),
					Map.of(PYTHON_ANALYZE_NODE, PYTHON_ANALYZE_NODE, END, END, PYTHON_GENERATE_NODE,
							PYTHON_GENERATE_NODE))
			// Python 分析完成后回到 PlanExecutor 继续执行下一个计划步骤
			.addEdge(PYTHON_ANALYZE_NODE, PLAN_EXECUTOR_NODE)

			// ===== PlanExecutor 是整个执行阶段的核心调度器 =====
			// 它根据当前计划步骤的类型路由到不同的处理节点：
			.addConditionalEdges(PLAN_EXECUTOR_NODE, edge_async(new PlanExecutorDispatcher()), Map.of(
					PLANNER_NODE, PLANNER_NODE,               // 计划校验失败 → 回到 Planner 重新生成（最多重试2次）
					SQL_GENERATE_NODE, SQL_GENERATE_NODE,     // SQL 类型步骤 → 进入 SQL 生成管道
					PYTHON_GENERATE_NODE, PYTHON_GENERATE_NODE, // Python 类型步骤 → 进入 Python 代码管道
					REPORT_GENERATOR_NODE, REPORT_GENERATOR_NODE, // 报告类型步骤 → 进入报告生成
					HUMAN_FEEDBACK_NODE, HUMAN_FEEDBACK_NODE, // 启用了人工审核 → 进入人工审核节点
					END, END))                                  // 计划执行完毕或超过最大修复次数 → 结束

			// ===== 人工审核节点路由 =====
			// HumanFeedback 支持"人在回路"（Human-in-the-loop）模式：
			// 图执行到此处会暂停，等待用户通过 API 提交审核反馈后继续
			.addConditionalEdges(HUMAN_FEEDBACK_NODE, edge_async(new HumanFeedbackDispatcher()), Map.of(
					PLANNER_NODE, PLANNER_NODE,           // 用户拒绝计划 → 回到 Planner 重新生成
					PLAN_EXECUTOR_NODE, PLAN_EXECUTOR_NODE, // 用户批准计划 → 继续执行
					END, END))                              // 超过最大修复次数 → 结束

			// 报告生成是工作流的最后一个节点，生成后直接结束
			.addEdge(REPORT_GENERATOR_NODE, END)

			// ===== SQL 生成与执行的闭环管道 =====
			// 这是一个带重试机制的三阶段管道：Generate → SemanticCheck → Execute
			// 任一阶段失败都会通过 Dispatcher 路由回 SqlGenerate 节点，携带错误信息让 LLM 修正 SQL
			// SqlGenerate 后的分支：Schema缺失→自身重试，生成成功→语义校验，不可恢复错误→END
			.addConditionalEdges(SQL_GENERATE_NODE, nodeBeanUtil.getEdgeBeanAsync(SqlGenerateDispatcher.class),
					Map.of(SQL_GENERATE_NODE, SQL_GENERATE_NODE, END, END, SEMANTIC_CONSISTENCY_NODE,
							SEMANTIC_CONSISTENCY_NODE))
			// 语义一致性校验后的分支：校验失败→回到 SqlGenerate 重新生成，校验通过→执行 SQL
			.addConditionalEdges(SEMANTIC_CONSISTENCY_NODE, edge_async(new SemanticConsistenceDispatcher()),
					Map.of(SQL_GENERATE_NODE, SQL_GENERATE_NODE, SQL_EXECUTE_NODE, SQL_EXECUTE_NODE))
			// SQL 执行后的分支：执行失败→回到 SqlGenerate 重新生成（携带错误信息），执行成功→回到 PlanExecutor 继续下一步
			.addConditionalEdges(SQL_EXECUTE_NODE, edge_async(new SQLExecutorDispatcher()),
					Map.of(SQL_GENERATE_NODE, SQL_GENERATE_NODE, PLAN_EXECUTOR_NODE, PLAN_EXECUTOR_NODE));

		// 将构建好的图导出为 PlantUML 格式并打印到日志，便于开发时可视化调试工作流拓扑
		GraphRepresentation graphRepresentation = stateGraph.getGraph(GraphRepresentation.Type.PLANTUML,
				"workflow graph");

		log.info("workflow in PlantUML format as follows \n\n" + graphRepresentation.content() + "\n\n");

		return stateGraph;
	}

	/**
	 * 配置内存向量存储（兜底方案）。
	 *
	 * <p>VectorStore 是 RAG（检索增强生成）的核心组件，用于存储文本的向量表示并支持相似度检索。
	 * 本项目默认使用 Spring AI 提供的 SimpleVectorStore（纯内存实现，重启后数据丢失），适用于开发环境。</p>
	 *
	 * <p><b>生产环境替换方式</b>：不要修改此方法，而是在 pom.xml 中引入持久化向量库的 Boot Starter
	 * （如 spring-ai-starter-vector-store-pgvector、milvus 等），Spring Boot 自动配置会自动替代此 Bean。
	 * 详见 <a href="https://springdoc.cn/spring-ai/api/vectordbs.html">Spring AI VectorStore 文档</a></p>
	 *
	 * @param embeddingModel 用于将文本转换为向量的嵌入模型
	 * @return SimpleVectorStore 实例
	 */
	@Primary
	@Bean
	@ConditionalOnMissingBean(VectorStore.class)
	@ConditionalOnProperty(name = "spring.ai.vectorstore.type", havingValue = "simple", matchIfMissing = true)
	public SimpleVectorStore simpleVectorStore(EmbeddingModel embeddingModel) {
		return SimpleVectorStore.builder(embeddingModel).build();
	}

	/**
	 * 初始化 SimpleVectorStore 的持久化数据加载。
	 *
	 * <p>SimpleVectorStore 支持将向量数据持久化到本地 JSON 文件。
	 * 此初始化器会在启动时从配置的文件路径加载已有的向量数据，避免每次重启都需要重新生成 embedding。</p>
	 *
	 * @param vectorStore SimpleVectorStore 实例
	 * @param properties  DataAgent 配置属性（包含向量存储的文件路径等配置）
	 * @return 初始化器实例
	 */
	@Bean
	@ConditionalOnBean(SimpleVectorStore.class)
	public SimpleVectorStoreInitialization simpleVectorStoreInitialization(SimpleVectorStore vectorStore,
			DataAgentProperties properties) {
		return new SimpleVectorStoreInitialization(vectorStore, properties);
	}

	/**
	 * 配置 Embedding 批处理策略。
	 *
	 * <p>当需要将大量文本转换为向量时，Embedding 模型的 API 通常有单次请求的 token 上限。
	 * BatchingStrategy 负责将一批文本按照 token 数量和文本数量拆分为多个小批次，
	 * 分批调用 Embedding API，避免超出模型的 token 限制。</p>
	 *
	 * <p>本项目使用自定义的 {@link EnhancedTokenCountBatchingStrategy}，
	 * 它同时考虑了 token 数量限制和文本数量限制，比 Spring AI 默认的策略更精细。</p>
	 *
	 * <p>EncodingType 指定了 tokenizer 的编码方式（如 CL100K_BASE 是 GPT-4/ChatGPT 使用的分词器），
	 * 用于准确计算文本的 token 数量。</p>
	 *
	 * @param properties DataAgent 配置属性（包含 embedding-batch 相关配置）
	 * @return BatchingStrategy 实例
	 */
	@Bean
	@ConditionalOnMissingBean(BatchingStrategy.class)
	public BatchingStrategy customBatchingStrategy(DataAgentProperties properties) {
		// 解析配置的 tokenizer 编码类型，默认使用 CL100K_BASE（GPT-4 系列的分词器）
		EncodingType encodingType;
		try {
			Optional<EncodingType> encodingTypeOptional = EncodingType
				.fromName(properties.getEmbeddingBatch().getEncodingType());
			encodingType = encodingTypeOptional.orElse(EncodingType.CL100K_BASE);
		}
		catch (Exception e) {
			log.warn("Unknown encodingType '{}', falling back to CL100K_BASE",
					properties.getEmbeddingBatch().getEncodingType());
			encodingType = EncodingType.CL100K_BASE;
		}

		return new EnhancedTokenCountBatchingStrategy(encodingType, properties.getEmbeddingBatch().getMaxTokenCount(),
				properties.getEmbeddingBatch().getReservePercentage(),
				properties.getEmbeddingBatch().getMaxTextCount());
	}

	/**
	 * 配置工具回调解析器，用于 MCP（Model Context Protocol）协议集成。
	 *
	 * <p>Spring AI 的 ToolCallback 机制允许将 Java 方法暴露为 LLM 可调用的"工具"（Tool），
	 * LLM 在生成回答时可以决定调用这些工具来获取额外信息。本解析器收集所有注册的 ToolCallback，
	 * 供 MCP Server 使用，使得外部 AI 客户端（如 Claude Desktop）可以通过 MCP 协议调用 DataAgent 的能力。</p>
	 *
	 * <p>解析器采用委托模式（Delegating），先从静态注册的 ToolCallback 中查找，再从 Spring Bean 容器中查找，
	 * 确保两种注册方式都能被找到。同时使用 McpServerToolUtil 排除由 MCP Server 自动注册的工具，
	 * 避免循环注册。</p>
	 *
	 * @param context Spring 应用上下文
	 * @return ToolCallbackResolver 实例
	 */
	@Bean
	public ToolCallbackResolver toolCallbackResolver(GenericApplicationContext context) {
		// 收集所有非 MCP Server 自动注册的 ToolCallback Bean
		List<ToolCallback> allFunctionAndToolCallbacks = new ArrayList<>(
				McpServerToolUtil.excludeMcpServerTool(context, ToolCallback.class));
		// 收集所有 ToolCallbackProvider Bean，并展开为 ToolCallback 列表
		McpServerToolUtil.excludeMcpServerTool(context, ToolCallbackProvider.class)
			.stream()
			.map(pr -> List.of(pr.getToolCallbacks()))
			.forEach(allFunctionAndToolCallbacks::addAll);

		// 静态解析器：基于预收集的 ToolCallback 列表
		var staticToolCallbackResolver = new StaticToolCallbackResolver(allFunctionAndToolCallbacks);

		// 动态解析器：基于 Spring Bean 容器延迟查找
		var springBeanToolCallbackResolver = SpringBeanToolCallbackResolver.builder()
			.applicationContext(context)
			.build();

		// 委托解析器：依次尝试静态解析和动态解析
		return new DelegatingToolCallbackResolver(List.of(staticToolCallbackResolver, springBeanToolCallbackResolver));
	}

	/**
	 * 创建 EmbeddingModel 的动态代理 Bean，实现模型运行时热切换。
	 *
	 * <p><b>为什么需要动态代理？</b></p>
	 * <p>向量数据库（如 Milvus、PgVector）的 Spring Boot Starter 在启动时需要一个 EmbeddingModel Bean
	 * 来完成初始化。但本系统支持运行时动态切换 Embedding 模型（通过 {@link AiModelRegistry}），
	 * 因此不能将 Bean 绑定到固定的模型实例。解决方案是使用 Spring AOP 的 TargetSource 机制：</p>
	 * <ol>
	 *   <li>创建一个代理 Bean 注册到 Spring 容器，让其他 Bean（如 VectorStore）可以注入它</li>
	 *   <li>代理内部通过 TargetSource 每次方法调用时都从 AiModelRegistry 获取当前最新的模型实例</li>
	 *   <li>这样当管理员通过 API 切换模型后，后续的 embedding 调用自动使用新模型</li>
	 * </ol>
	 *
	 * @param registry AI 模型注册表，负责管理当前活跃的 Embedding 模型实例
	 * @return EmbeddingModel 的动态代理对象
	 */
	@Bean
	@Primary
	public EmbeddingModel embeddingModel(AiModelRegistry registry) {

		// 1. 定义 TargetSource（目标源）-- 控制代理对象每次方法调用的实际目标
		TargetSource targetSource = new TargetSource() {
			@Override
			public Class<?> getTargetClass() {
				return EmbeddingModel.class;
			}

			@Override
			public boolean isStatic() {
				// 关键：返回 false 表示每次方法调用都要重新获取目标对象（动态代理）
				return false;
			}

			@Override
			public Object getTarget() {
				// 每次方法调用时，从注册表获取当前最新配置的 Embedding 模型
				return registry.getEmbeddingModel();
			}

			@Override
			public void releaseTarget(Object target) {
				// 无需释放，模型实例由 AiModelRegistry 管理
			}
		};

		// 2. 创建代理工厂并配置
		ProxyFactory proxyFactory = new ProxyFactory();
		proxyFactory.setTargetSource(targetSource);
		proxyFactory.addInterface(EmbeddingModel.class); // 基于 JDK 动态代理（接口代理）

		// 3. 生成并返回代理对象
		return (EmbeddingModel) proxyFactory.getProxy();
	}

	/**
	 * 创建专用于数据库操作的线程池。
	 *
	 * <p>该线程池主要用于 Schema 信息查询等数据库 IO 密集型操作的并行处理。
	 * 核心线程数设置为 CPU 核心数的 2 倍（IO 密集型场景下线程常处于等待状态，可适当增加并发度），
	 * 上限为 16。使用有界队列（容量 500）防止内存溢出，
	 * 拒绝策略为 CallerRunsPolicy（队列满时由调用线程执行，起到自然限流的作用）。</p>
	 *
	 * @return 数据库操作专用线程池
	 */
	@Bean(name = "dbOperationExecutor")
	public ExecutorService dbOperationExecutor() {
		int corePoolSize = Math.max(4, Math.min(Runtime.getRuntime().availableProcessors() * 2, 16));
		log.info("Database operation executor initialized with {} threads", corePoolSize);

		// 自定义线程工厂：为线程设置可辨识的名称（db-operation-N），便于日志排查和线程 dump 分析
		ThreadFactory threadFactory = new ThreadFactory() {
			private final AtomicInteger threadNumber = new AtomicInteger(1);

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "db-operation-" + threadNumber.getAndIncrement());
				t.setDaemon(false);
				if (t.getPriority() != Thread.NORM_PRIORITY) {
					t.setPriority(Thread.NORM_PRIORITY);
				}
				return t;
			}
		};

		this.dbOperationExecutor = new ThreadPoolExecutor(corePoolSize, corePoolSize, 60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(500), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

		return dbOperationExecutor;
	}

	/**
	 * 应用关闭时优雅停止数据库操作线程池。
	 *
	 * <p>关闭流程遵循标准的优雅停机模式：先停止接收新任务 → 等待已提交任务完成（60秒超时）
	 * → 强制关闭（中断所有正在执行的任务）→ 再次确认（10秒超时）。
	 * 实现了 {@link DisposableBean} 接口，Spring 容器关闭时会自动回调此方法。</p>
	 */
	@Override
	public void destroy() {
		if (dbOperationExecutor != null && !dbOperationExecutor.isShutdown()) {
			log.info("Shutting down database operation executor...");

			// 记录关闭前的状态，便于排查未完成任务的问题
			if (dbOperationExecutor instanceof ThreadPoolExecutor tpe) {
				log.info("Executor Status before shutdown: [Queue Size: {}], [Active Count: {}], [Completed Tasks: {}]",
						tpe.getQueue().size(), tpe.getActiveCount(), tpe.getCompletedTaskCount());
			}

			// 1. 停止接收新任务
			dbOperationExecutor.shutdown();

			try {
				// 2. 等待现有任务完成（包括队列中的）
				if (!dbOperationExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
					log.warn("Executor did not terminate in 60s. Forcing shutdown...");

					// 3. 超时强行关闭
					dbOperationExecutor.shutdownNow();

					// 4. 再次确认是否关闭
					if (!dbOperationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
						log.error("Executor failed to terminate completely.");
					}
				}
				else {
					log.info("Database operation executor terminated gracefully.");
				}
			}
			catch (InterruptedException e) {
				log.warn("Interrupted during executor shutdown. Forcing immediate shutdown.");
				dbOperationExecutor.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}

	// ===== 文本分块器（TextSplitter）配置 =====
	// 文本分块器用于将长文档切分为多个小块（chunk），以便 Embedding 模型处理。
	// 不同的分块策略适用于不同的场景，通过 spring.ai.alibaba.data-agent.text-splitter.type 配置选择。
	// 所有分块器 Bean 都实现了 TextSplitter 接口，通过 Bean 名称区分策略类型。

	/**
	 * Token 分块器 -- 基于 token 数量进行分块。
	 *
	 * <p>使用 OpenAI 的 tokenizer（如 CL100K_BASE）精确计算 token 数量，确保每个 chunk 不超过模型的
	 * 最大 token 限制。这是最常用的分块策略，适用于大多数场景。</p>
	 *
	 * @param properties 分块配置
	 * @return TokenTextSplitter 实例
	 */
	@Bean(name = "token")
	public TextSplitter textSplitter(DataAgentProperties properties) {
		DataAgentProperties.TextSplitter textSplitterProps = properties.getTextSplitter();
		DataAgentProperties.TextSplitter.TokenTextSplitterConfig config = textSplitterProps.getToken();
		return new TokenTextSplitter(textSplitterProps.getChunkSize(), config.getMinChunkSizeChars(),
				config.getMinChunkLengthToEmbed(), config.getMaxNumChunks(), config.isKeepSeparator());
	}

	/**
	 * 递归字符分块器 -- 按分隔符层级递归切分。
	 *
	 * <p>先尝试用第一级分隔符（如双换行 "\n\n"）切分，如果某块仍然过大，
	 * 则用下一级分隔符（如单换行 "\n"、空格等）继续切分，直到满足大小要求。
	 * 这种方式能尽量保持段落的语义完整性。</p>
	 *
	 * @param properties 分块配置
	 * @return RecursiveCharacterTextSplitter 实例
	 */
	@Bean(name = "recursive")
	public TextSplitter recursiveTextSplitter(DataAgentProperties properties) {
		DataAgentProperties.TextSplitter textSplitterProps = properties.getTextSplitter();
		DataAgentProperties.TextSplitter.RecursiveTextSplitterConfig config = textSplitterProps.getRecursive();
		String[] separators = config.getSeparators();
		if (separators != null && separators.length > 0) {
			return new RecursiveCharacterTextSplitter(textSplitterProps.getChunkSize(), separators);
		}
		else {
			return new RecursiveCharacterTextSplitter(textSplitterProps.getChunkSize());
		}
	}

	/**
	 * 句子分块器 -- 以句子为基本单位进行分块。
	 *
	 * <p>将文本按句子边界切分，支持设置句子间的重叠度（overlap），
	 * 以确保分块边界处的上下文信息不会丢失。</p>
	 *
	 * @param properties 分块配置
	 * @return SentenceSplitter 实例
	 */
	@Bean(name = "sentence")
	public TextSplitter sentenceSplitter(DataAgentProperties properties) {
		DataAgentProperties.TextSplitter textSplitterConfig = properties.getTextSplitter();
		DataAgentProperties.TextSplitter.SentenceTextSplitterConfig sentenceConfig = textSplitterConfig.getSentence();

		return SentenceSplitter.builder()
			.withChunkSize(textSplitterConfig.getChunkSize())
			.withSentenceOverlap(sentenceConfig.getSentenceOverlap())
			.build();
	}

	/**
	 * 语义分块器 -- 基于语义相似度进行智能分块。
	 *
	 * <p>利用 EmbeddingModel 计算相邻文本片段的语义相似度，在语义发生显著变化的位置进行切分。
	 * 这种方式能产生语义上最连贯的分块，但需要额外的 Embedding API 调用，成本较高。</p>
	 *
	 * @param properties     分块配置
	 * @param embeddingModel 用于计算语义相似度的嵌入模型
	 * @return SemanticTextSplitter 实例
	 */
	@Bean(name = "semantic")
	public TextSplitter semanticSplitter(DataAgentProperties properties, EmbeddingModel embeddingModel) {
		DataAgentProperties.TextSplitter textSplitterProps = properties.getTextSplitter();
		DataAgentProperties.TextSplitter.SemanticTextSplitterConfig config = textSplitterProps.getSemantic();
		return SemanticTextSplitter.builder()
			.embeddingModel(embeddingModel)
			.minChunkSize(config.getMinChunkSize())
			.maxChunkSize(config.getMaxChunkSize())
			.similarityThreshold(config.getSimilarityThreshold())
			.build();
	}

	/**
	 * 段落分块器 -- 以段落为基本单位进行分块。
	 *
	 * <p>按段落边界（如空行）切分文本，支持设置段落间的字符重叠度。
	 * 适用于结构化文档（如 Markdown）的场景。</p>
	 *
	 * @param properties 分块配置
	 * @return ParagraphTextSplitter 实例
	 */
	@Bean(name = "paragraph")
	public TextSplitter paragraphSplitter(DataAgentProperties properties) {
		DataAgentProperties.TextSplitter textSplitterProps = properties.getTextSplitter();
		DataAgentProperties.TextSplitter.ParagraphTextSplitterConfig config = textSplitterProps.getParagraph();
		return ParagraphTextSplitter.builder()
			.chunkSize(textSplitterProps.getChunkSize())
			.paragraphOverlapChars(config.getParagraphOverlapChars())
			.build();
	}

}
