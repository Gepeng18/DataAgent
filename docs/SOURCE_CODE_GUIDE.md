# DataAgent 源码导读：跟着一个请求走完全流程

> 本文档以用户输入 **"各省销售额排名"** 为例，从源码层面完整跟踪一个请求从浏览器发起到最终报告呈现的全过程。适合有 Java/Spring 基础的开发者快速理解项目架构。

---

## 目录

- [全局架构概览](#全局架构概览)
- [第 1 站：前端发出请求](#第-1-站前端发出请求)
- [第 2 站：后端接收请求](#第-2-站后端接收请求)
- [第 3 站：StateGraph 工作流引擎](#第-3-站stategraph-工作流引擎)
- [第 4 站：节点逐一讲解](#第-4-站节点逐一讲解)
  - [4.1 IntentRecognitionNode — 意图识别](#41-intentrecognitionnode--意图识别)
  - [4.2 EvidenceRecallNode — RAG 知识检索](#42-evidencerecallnode--rag-知识检索)
  - [4.3 QueryEnhanceNode — 查询增强](#43-queryenhancenode--查询增强)
  - [4.4 SchemaRecallNode — 表结构召回](#44-schemarecallnode--表结构召回)
  - [4.5 TableRelationNode — 构建完整 Schema](#45-tablerelationnode--构建完整-schema)
  - [4.6 FeasibilityAssessmentNode — 可行性评估](#46-feasibilityassessmentnode--可行性评估)
  - [4.7 PlannerNode — 生成执行计划](#47-plannernode--生成执行计划)
  - [4.8 PlanExecutorNode — 计划执行调度器](#48-planexecutornode--计划执行调度器)
  - [4.9 SqlGenerateNode — SQL 生成](#49-sqlgeneratenode--sql-生成)
  - [4.10 SemanticConsistencyNode — SQL 语义检查](#410-semanticconsistencynode--sql-语义检查)
  - [4.11 SqlExecuteNode — SQL 执行](#411-sqlexecutenode--sql-执行)
  - [4.12 PythonGenerateNode — Python 代码生成](#412-pythongeneratenode--python-代码生成)
  - [4.13 PythonExecuteNode — Python 代码执行](#413-pythonexecutenode--python-代码执行)
  - [4.14 PythonAnalyzeNode — Python 结果分析](#414-pythonanalyzenode--python-结果分析)
  - [4.15 ReportGeneratorNode — 报告生成](#415-reportgeneratornode--报告生成)
  - [4.16 HumanFeedbackNode — 人工审核](#416-humanfeedbacknode--人工审核)
- [第 5 站：结果流回前端](#第-5-站结果流回前端)
- [辅助体系详解](#辅助体系详解)
  - [Prompt 模板体系](#prompt-模板体系)
  - [工具类体系](#工具类体系)
  - [多数据库连接层](#多数据库连接层)
  - [Python 容器池](#python-容器池)
- [关键设计模式总结](#关键设计模式总结)
- [调试与学习建议](#调试与学习建议)

---

## 全局架构概览

### 技术栈

| 层面 | 技术 | 说明 |
|---|---|---|
| 后端框架 | Spring Boot 3.4 + WebFlux | 响应式 Web 框架，基于 Reactor |
| 工作流引擎 | Spring AI Alibaba Graph（StateGraph） | 自研的有向图状态机 |
| ORM | MyBatis | 元数据持久化（Agent、数据源、知识库等） |
| LLM 接入 | Spring AI + OpenAI 兼容协议 | 统一的 ChatClient 接口，可接任何大模型 |
| 向量检索 | Spring AI VectorStore | RAG 检索增强生成 |
| 前端 | Vue 3 + Element Plus + ECharts | Vite 构建，SSE 实时通信 |
| Python 执行 | Docker 容器池 | LLM 生成代码，沙箱中执行 |

### 完整流程图

```
用户输入 "各省销售额排名"
│
├─── 前端 (AgentRun.vue:582)
│    sendMessage() → GraphService.streamSearch() → EventSource SSE连接
│    URL: GET /api/stream/search?query=各省销售额排名
│
├─── 后端入口 (GraphController.java:46)
│    创建 SSE Sink → GraphServiceImpl.graphStreamProcess()
│    → 构建初始状态Map → compiledGraph.stream() 启动工作流
│
├─── 工作流 16 个节点依次执行
│
│    ① IntentRecognition   → 判定 "是数据分析请求"
│    ② EvidenceRecall      → RAG 检索业务知识
│    ③ QueryEnhance        → 改写为标准查询
│    ④ SchemaRecall        → 向量搜索找到相关表
│    ⑤ TableRelation       → 组装 SchemaDTO + LLM 精选
│    ⑥ FeasibilityAssess   → 判定 "可以回答"
│    ⑦ Planner             → 生成多步执行计划
│    ⑧ PlanExecutor        → 调度: Step1=SQL, Step2=报告
│    ⑨ SqlGenerate         → LLM 生成 SQL
│    ⑩ SemanticConsistency → LLM 审查 SQL → 通过
│    ⑪ SqlExecute          → JDBC 执行 → 返回结果集
│    ⑫ PlanExecutor        → 推进到 Step2
│    ⑬ ReportGenerator     → 组装 Markdown 报告（含 ECharts 图表）
│    ⑭ END
│
├─── 流式回传 (GraphServiceImpl.java:287)
│    每个节点每个 token → StreamingOutput → SSE 事件 → 浏览器
│    前端按 textType 实时渲染（SQL 高亮、表格、Markdown 报告）
│
└─── 前端渲染完成，用户看到分析报告
```

---

## 第 1 站：前端发出请求

### 1.1 用户输入 → 构建请求

**文件**: `data-agent-frontend/src/views/AgentRun.vue:582-624`

用户在聊天框输入 "各省销售额排名"，按回车或点击发送按钮，触发 `sendMessage()` 方法：

```javascript
// AgentRun.vue:582-624
const sendMessage = async () => {
  if (!userInput.value.trim()) return;

  const request: GraphRequest = {
    agentId: route.params.id,    // 从路由获取 Agent ID
    query: userInput.value,      // "各省销售额排名"
    threadId: currentThreadId,   // 会话线程 ID（用于多轮对话）
    humanFeedback: false,
    nl2sqlOnly: false,
  };
  await sendGraphRequest(request, true);
};
```

### 1.2 建立 SSE 连接

**文件**: `data-agent-frontend/src/services/graph.ts:58-124`

`sendGraphRequest()` 调用 `GraphService.streamSearch()`，通过浏览器原生 `EventSource` 建立 SSE（Server-Sent Events）连接。

> **SSE 是什么？** SSE 是一种基于 HTTP 的单向推送协议。服务器可以持续向浏览器推送数据，浏览器通过 `onmessage` 回调接收。不同于 WebSocket 的全双工通信，SSE 只能服务器→客户端，但实现更简单，天然支持断线重连。

```typescript
// graph.ts:58-124
const streamSearch = (request: GraphRequest, callbacks: {...}) => {
  // 构建请求 URL
  const params = new URLSearchParams();
  params.set('agentId', request.agentId);
  params.set('query', request.query);
  params.set('threadId', request.threadId || '');
  // ...

  // 创建 SSE 连接
  const url = `/api/stream/search?${params.toString()}`;
  const eventSource = new EventSource(url);

  // 每收到一个 SSE 事件就回调
  eventSource.onmessage = (event) => {
    const response: GraphNodeResponse = JSON.parse(event.data);
    callbacks.onMessage(response);
  };

  // 监听完成事件
  eventSource.addEventListener('complete', () => {
    eventSource.close();
    callbacks.onComplete();
  });
};
```

SSE 事件的响应格式 `GraphNodeResponse`（定义在 `graph.ts:27-35`）：

```typescript
interface GraphNodeResponse {
  agentId: string;       // Agent ID
  threadId: string;      // 会话线程 ID
  nodeName: string;      // 当前执行的节点名（如 "SqlGenerateNode"）
  textType: TextType;    // 内容类型：SQL / JSON / PYTHON / MARK_DOWN / ...
  text: string;          // 文本内容（每个 SSE 事件包含一小段）
  error: boolean;        // 是否出错
  complete: boolean;     // 是否完成
}
```

---

## 第 2 站：后端接收请求

### 2.1 GraphController — SSE 端点

**文件**: `data-agent-management/src/main/java/com/alibaba/cloud/ai/dataagent/controller/GraphController.java:46-93`

```java
@RestController
@RequestMapping("/api")
public class GraphController {

    @GetMapping("/stream/search")
    public Flux<ServerSentEvent<GraphNodeResponse>> streamSearch(
            @RequestParam String agentId,
            @RequestParam String threadId,
            @RequestParam String query,
            @RequestParam(required = false, defaultValue = "false") boolean humanFeedback,
            @RequestParam(required = false) String humanFeedbackContent,
            @RequestParam(required = false, defaultValue = "false") boolean rejectedPlan,
            @RequestParam(required = false, defaultValue = "false") boolean nl2sqlOnly) {

        // 1. 创建 SSE 推送管道
        Sinks.Many<ServerSentEvent<GraphNodeResponse>> sink =
            Sinks.many().unicast().onBackpressureBuffer();

        // 2. 构建请求 DTO
        GraphRequest request = new GraphRequest(agentId, threadId, query,
            humanFeedback, humanFeedbackContent, rejectedPlan, nl2sqlOnly);

        // 3. 异步启动图执行（关键：不阻塞当前 HTTP 线程）
        graphService.graphStreamProcess(sink, request);

        // 4. 返回 Flux，Spring WebFlux 自动将 Flux 转为 SSE 流推给浏览器
        return sink.asFlux()
            .filter(sse -> sse.data() != null)
            .doOnCancel(() -> graphService.stopStreamProcessing(threadId))
            .doOnError(e -> graphService.stopStreamProcessing(threadId));
    }
}
```

**关键设计解析**：

- `Sinks.Many<T>` 是 Project Reactor 提供的多播管道。可以把它理解为一个线程安全的"广播站"——一个线程往里面塞数据（`sink.tryEmitNext()`），另一个线程（Spring WebFlux 的 HTTP 线程）从里面取数据并推给浏览器。
- `graphService.graphStreamProcess()` 在独立线程池中执行工作流，不阻塞 HTTP 响应。HTTP 响应立即返回 `Flux<ServerSentEvent>`，后续数据异步推送。
- `doOnCancel` 和 `doOnError` 处理客户端断连场景，清理资源。

### 2.2 GraphServiceImpl — 启动图执行

**文件**: `data-agent-management/src/main/java/com/alibaba/cloud/ai/dataagent/service/graph/GraphServiceImpl.java`

这是整个请求处理的核心引擎。它管理 SSE 连接的生命周期，并协调工作流的执行。

#### 构造函数 — 编译图

```java
// GraphServiceImpl.java:61-68
public GraphServiceImpl(StateGraph stateGraph, ...) {
    // 编译图，设置中断点：执行到 HumanFeedbackNode 之前暂停
    this.compiledGraph = stateGraph.compile(
        CompileConfig.builder()
            .interruptBefore(HUMAN_FEEDBACK_NODE)
            .build()
    );
}
```

`compiledGraph` 是一个不可变的、优化过的可执行图。`interruptBefore` 配置使得当流程到达 `HumanFeedbackNode` 时自动暂停，等待人工审核后再恢复。

#### graphStreamProcess — 主入口

```java
// GraphServiceImpl.java:80-94
public void graphStreamProcess(Sinks.Many<...> sink, GraphRequest request) {
    String threadId = request.threadId();
    if (StringUtils.isBlank(threadId)) {
        threadId = UUID.randomUUID().toString();  // 首次请求生成线程 ID
    }

    // 创建流上下文（保存 sink、订阅句柄、Langfuse 追踪信息）
    StreamContext context = streamContextMap
        .computeIfAbsent(threadId, k -> new StreamContext());
    context.setSink(sink);

    // 分支判断：是人工反馈恢复，还是全新请求
    if (request.humanFeedbackContent() != null) {
        handleHumanFeedback(context, request);   // 恢复被中断的图
    } else {
        handleNewProcess(context, request);       // 全新执行
    }
}
```

`streamContextMap` 是一个 `ConcurrentHashMap<String, StreamContext>`，以 `threadId` 为 key 管理所有活跃的 SSE 连接。每个会话有独立的流上下文。

#### handleNewProcess — 启动全新执行

```java
// GraphServiceImpl.java:118-147
private void handleNewProcess(StreamContext context, GraphRequest request) {
    String query = request.query();
    String agentId = request.agentId();
    String threadId = request.threadId();

    // 1. 构建多轮对话上下文
    String multiTurnContext = multiTurnContextManager.buildContext(threadId);
    multiTurnContextManager.beginTurn(threadId, query);

    // 2. 构建初始状态 Map（工作流的全局状态）
    Map<String, Object> stateMap = Map.of(
        "input",                 query,              // "各省销售额排名"
        "agentId",               agentId,            // Agent ID
        "IS_ONLY_NL2SQL",        false,              // 是否纯 SQL 模式
        "HUMAN_REVIEW_ENABLED",  false,              // 是否开启人工审核
        "MULTI_TURN_CONTEXT",    multiTurnContext,    // 多轮对话历史
        "TRACE_THREAD_ID",       threadId            // 追踪 ID
    );

    // 3. 启动工作流执行
    Flux<NodeOutput> nodeOutputFlux = compiledGraph.stream(
        stateMap,
        RunnableConfig.builder().threadId(threadId).build()
    );

    // 4. 订阅流，处理每个节点的输出
    subscribeToFlux(context, nodeOutputFlux, ...);
}
```

**初始状态 Map 的含义**：这是 StateGraph 的全局状态，所有节点共享。每个节点从中读取输入，把输出写回。状态键使用 `KeyStrategy.REPLACE` 策略，意味着后写入的值会覆盖先写入的值。

#### handleHumanFeedback — 恢复人工审核

```java
// GraphServiceImpl.java:149-191
private void handleHumanFeedback(StreamContext context, GraphRequest request) {
    // 1. 构建反馈数据
    Map<String, Object> feedbackData = Map.of(
        "feedback", !request.rejectedPlan(),
        "feedback_content", request.humanFeedbackContent()
    );

    // 2. 更新图状态（注入反馈数据）
    compiledGraph.updateState(
        RunnableConfig.builder().threadId(request.threadId()).build(),
        Map.of(HUMAN_FEEDBACK_DATA, feedbackData, MULTI_TURN_CONTEXT, ...)
    );

    // 3. 从中断点恢复执行（传入 null 表示不重新开始，而是从中断处继续）
    Flux<NodeOutput> nodeOutputFlux = compiledGraph.stream(
        null,  // null = 恢复模式，不从初始状态开始
        resumeConfig
    );

    subscribeToFlux(context, nodeOutputFlux, ...);
}
```

人工审核机制依赖 `compiledGraph` 的 `interruptBefore` 配置。当流程到达 `HumanFeedbackNode` 时，图会暂停执行，`NodeOutput` 流会自然结束。用户在前端点击"批准"或"拒绝"后，前端再次发起 SSE 请求，`handleHumanFeedback` 通过 `updateState()` 注入反馈数据，然后调用 `compiledGraph.stream(null, resumeConfig)` 恢复执行。

---

## 第 3 站：StateGraph 工作流引擎

### 3.1 图的定义 — DataAgentConfiguration

**文件**: `data-agent-management/src/main/java/com/alibaba/cloud/ai/dataagent/config/DataAgentConfiguration.java:118-261`

这是 Spring 的 `@Bean` 方法，在应用启动时构建整个工作流图。StateGraph 是 Spring AI Alibaba 提供的自研图引擎，不是 LangChain4j 或其他第三方库。

#### 状态键定义

```java
// DataAgentConfiguration.java:121-182
KeyStrategyFactory keyStrategyFactory = new KeyStrategyFactory(Map.ofEntries(
    // 输入
    entry(INPUT_KEY,                  KeyStrategy.REPLACE),  // "input"
    entry(AGENT_ID,                   KeyStrategy.REPLACE),  // "agentId"
    entry(MULTI_TURN_CONTEXT,         KeyStrategy.REPLACE),
    // 意图识别
    entry(INTENT_RECOGNITION_NODE_OUTPUT, KeyStrategy.REPLACE),
    // 证据召回
    entry(EVIDENCE,                   KeyStrategy.REPLACE),
    // 查询增强
    entry(QUERY_ENHANCE_NODE_OUTPUT,  KeyStrategy.REPLACE),
    // Schema 召回
    entry(TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT,  KeyStrategy.REPLACE),
    entry(COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT, KeyStrategy.REPLACE),
    // 表关系
    entry(TABLE_RELATION_OUTPUT,      KeyStrategy.REPLACE),
    entry(DB_DIALECT_TYPE,            KeyStrategy.REPLACE),
    // 可行性评估
    entry(FEASIBILITY_ASSESSMENT_NODE_OUTPUT, KeyStrategy.REPLACE),
    // 计划器
    entry(PLANNER_NODE_OUTPUT,        KeyStrategy.REPLACE),
    entry(PLAN_CURRENT_STEP,          KeyStrategy.REPLACE),
    entry(PLAN_NEXT_NODE,             KeyStrategy.REPLACE),
    entry(PLAN_VALIDATION_STATUS,     KeyStrategy.REPLACE),
    // SQL 生成/执行
    entry(SQL_GENERATE_OUTPUT,        KeyStrategy.REPLACE),
    entry(SQL_GENERATE_COUNT,         KeyStrategy.REPLACE),
    entry(SQL_REGENERATE_REASON,      KeyStrategy.REPLACE),
    entry(SEMANTIC_CONSISTENCY_NODE_OUTPUT, KeyStrategy.REPLACE),
    entry(SQL_EXECUTE_NODE_OUTPUT,    KeyStrategy.REPLACE),
    entry(SQL_RESULT_LIST_MEMORY,     KeyStrategy.REPLACE),
    // Python 生成/执行
    entry(PYTHON_GENERATE_NODE_OUTPUT, KeyStrategy.REPLACE),
    entry(PYTHON_EXECUTE_NODE_OUTPUT,  KeyStrategy.REPLACE),
    entry(PYTHON_TRIES_COUNT,         KeyStrategy.REPLACE),
    entry(PYTHON_IS_SUCCESS,          KeyStrategy.REPLACE),
    entry(PYTHON_FALLBACK_MODE,       KeyStrategy.REPLACE),
    // 最终结果
    entry(RESULT,                     KeyStrategy.REPLACE),
    // ... 共 45+ 个状态键
));
```

#### 节点注册（16 个）

```java
// DataAgentConfiguration.java:185-200
StateGraph graph = new StateGraph("nl2sqlGraph", keyStrategyFactory);

graph.addNode(INTENT_RECOGNITION_NODE,     intentRecognitionNode);      // 1
graph.addNode(EVIDENCE_RECALL_NODE,        evidenceRecallNode);         // 2
graph.addNode(QUERY_ENHANCE_NODE,          queryEnhanceNode);           // 3
graph.addNode(SCHEMA_RECALL_NODE,          schemaRecallNode);           // 4
graph.addNode(TABLE_RELATION_NODE,         tableRelationNode);          // 5
graph.addNode(FEASIBILITY_ASSESSMENT_NODE, feasibilityAssessmentNode);  // 6
graph.addNode(SQL_GENERATE_NODE,           sqlGenerateNode);            // 7
graph.addNode(PLANNER_NODE,                plannerNode);                // 8
graph.addNode(PLAN_EXECUTOR_NODE,          planExecutorNode);           // 9
graph.addNode(SQL_EXECUTE_NODE,            sqlExecuteNode);             // 10
graph.addNode(PYTHON_GENERATE_NODE,        pythonGenerateNode);         // 11
graph.addNode(PYTHON_EXECUTE_NODE,         pythonExecuteNode);          // 12
graph.addNode(PYTHON_ANALYZE_NODE,         pythonAnalyzeNode);          // 13
graph.addNode(REPORT_GENERATOR_NODE,       reportGeneratorNode);        // 14
graph.addNode(SEMANTIC_CONSISTENCY_NODE,   semanticConsistencyNode);    // 15
graph.addNode(HUMAN_FEEDBACK_NODE,         humanFeedbackNode);          // 16
```

#### 边注册 — 完整拓扑

```java
// 固定边：START → 第一个节点
graph.addEdge(START, INTENT_RECOGNITION_NODE);

// 条件边：意图识别后根据分类结果路由
graph.addConditionalEdges(INTENT_RECOGNITION_NODE,
    edge_async(intentRecognitionDispatcher),
    Map.of(EVIDENCE_RECALL_NODE, evidenceRecallNode, END, END));

// 固定边：证据召回 → 查询增强
graph.addEdge(EVIDENCE_RECALL_NODE, QUERY_ENHANCE_NODE);

// 条件边：查询增强后路由
graph.addConditionalEdges(QUERY_ENHANCE_NODE,
    edge_async(queryEnhanceDispatcher),
    Map.of(SCHEMA_RECALL_NODE, schemaRecallNode, END, END));

// 条件边：Schema 召回后路由
graph.addConditionalEdges(SCHEMA_RECALL_NODE,
    edge_async(schemaRecallDispatcher),
    Map.of(TABLE_RELATION_NODE, tableRelationNode, END, END));

// 条件边：表关系后路由（可重试）
graph.addConditionalEdges(TABLE_RELATION_NODE,
    edge_async(tableRelationDispatcher),
    Map.of(FEASIBILITY_ASSESSMENT_NODE, feasibilityAssessmentNode,
           END, END,
           TABLE_RELATION_NODE, tableRelationNode));

// 条件边：可行性评估后路由
graph.addConditionalEdges(FEASIBILITY_ASSESSMENT_NODE,
    edge_async(feasibilityAssessmentDispatcher),
    Map.of(PLANNER_NODE, plannerNode, END, END));

// 固定边：计划器 → 计划执行器
graph.addEdge(PLANNER_NODE, PLAN_EXECUTOR_NODE);

// 条件边：计划执行器可路由到 SQL/Python/报告/人工审核
graph.addConditionalEdges(PLAN_EXECUTOR_NODE,
    edge_async(planExecutorDispatcher),
    Map.of(PLANNER_NODE, plannerNode,
           SQL_GENERATE_NODE, sqlGenerateNode,
           PYTHON_GENERATE_NODE, pythonGenerateNode,
           REPORT_GENERATOR_NODE, reportGeneratorNode,
           HUMAN_FEEDBACK_NODE, humanFeedbackNode,
           END, END));

// SQL 路径：生成 → 语义检查 → 执行
graph.addConditionalEdges(SQL_GENERATE_NODE,
    edge_async(sqlGenerateDispatcher),
    Map.of(SQL_GENERATE_NODE, sqlGenerateNode, END, END,
           SEMANTIC_CONSISTENCY_NODE, semanticConsistencyNode));

graph.addConditionalEdges(SEMANTIC_CONSISTENCY_NODE,
    edge_async(semanticConsistenceDispatcher),
    Map.of(SQL_GENERATE_NODE, sqlGenerateNode,
           SQL_EXECUTE_NODE, sqlExecuteNode));

graph.addConditionalEdges(SQL_EXECUTE_NODE,
    edge_async(sqlExecutorDispatcher),
    Map.of(SQL_GENERATE_NODE, sqlGenerateNode,
           PLAN_EXECUTOR_NODE, planExecutorNode));

// Python 路径：生成 → 执行 → 分析
graph.addEdge(PYTHON_GENERATE_NODE, PYTHON_EXECUTE_NODE);

graph.addConditionalEdges(PYTHON_EXECUTE_NODE,
    edge_async(pythonExecutorDispatcher),
    Map.of(PYTHON_ANALYZE_NODE, pythonAnalyzeNode, END, END,
           PYTHON_GENERATE_NODE, pythonGenerateNode));

graph.addEdge(PYTHON_ANALYZE_NODE, PLAN_EXECUTOR_NODE);

// 人工审核路径
graph.addConditionalEdges(HUMAN_FEEDBACK_NODE,
    edge_async(humanFeedbackDispatcher),
    Map.of(PLANNER_NODE, plannerNode,
           PLAN_EXECUTOR_NODE, planExecutorNode, END, END));

// 报告生成 → 结束
graph.addEdge(REPORT_GENERATOR_NODE, END);
```

#### 拓扑图

下图展示了 `DataAgentConfiguration.nl2sqlGraph()` 中定义的完整有向图拓扑。实线箭头 `──▶` 表示固定边（无条件跳转），虚线箭头 `╌╌▶` 表示条件边（由 Dispatcher 根据状态动态路由）。

```
                              ┌─────────┐
                              │  START   │
                              └────┬─────┘
                                   │
                                   ▼
                      ┌────────────────────────┐
                      │ ① IntentRecognitionNode│  意图识别
                      └────────────┬───────────┘
                                   │
                          ┌────────┴────────┐
                          │ Dispatcher:      │
                          │ 数据分析? 闲聊?  │
                   ┌──────┴──────┐     ┌────┴────┐
                   │ 数据分析     │     │ 闲聊    │
                   ▼              │     ▼         │
          ┌────────────────┐     │   ┌───┐       │
          │②EvidenceRecall │     │   │END│       │
          │     Node       │     │   └───┘       │
          └───────┬────────┘     │               │
                  │ (固定边)      │               │
                  ▼              │               │
       ┌──────────────────┐     │               │
       │③QueryEnhanceNode │     │               │
       └────────┬─────────┘     │               │
                │               │               │
        ┌───────┴───────┐       │               │
        │ Dispatcher:    │       │               │
   ┌────┴────┐    ┌─────┴──┐    │               │
   │ 继续    │    │ 结束   │    │               │
   ▼         │    ▼        │    │               │
┌────────────────────┐  ┌───┐   │               │
│④ SchemaRecallNode  │  │END│   │               │
└─────────┬──────────┘  └───┘   │               │
          │                     │               │
    ┌─────┴──────┐              │               │
    │ Dispatcher │              │               │
┌───┴────┐ ┌────┴──┐           │               │
│ 继续   │ │ 结束  │           │               │
▼        │ ▼       │           │               │
┌────────────────────┐ ┌───┐   │               │
│⑤ TableRelationNode │ │END│   │               │
└─────────┬──────────┘ └───┘   │               │
          │                    │               │
    ┌─────┴──────────┐         │               │
    │ Dispatcher:     │         │               │
    │ 通过/失败/重试  │         │               │
┌───┴──────┐ ┌──────┴──┐      │               │
│ 通过     │ │重试(自环)│      │               │
▼          │ │         │      │               │
┌─────────────────────────────┐│               │
│⑥ FeasibilityAssessmentNode  ││               │
│          可行性评估          ││               │
└─────────────┬───────────────┘│               │
              │                │               │
       ┌──────┴──────┐        │               │
       │ Dispatcher: │        │               │
  ┌────┴─────┐ ┌────┴──┐     │               │
  │ 可行     │ │不可行 │     │               │
  ▼          │ ▼       │     │               │
┌─────────────────┐  ┌───┐   │               │
│⑦ PlannerNode    │  │END│   │               │
│   生成执行计划   │  └───┘   │               │
└────────┬────────┘          │               │
         │ (固定边)           │               │
         ▼                   │               │
┌────────────────────┐       │               │
│⑧ PlanExecutorNode  │◀──────────────────────────────────┐
│  验证计划 & 推进步骤│       │               │           │
└────────┬───────────┘       │               │           │
         │                   │               │           │
  ┌──────┼───────┬───────────┼───────┐       │           │
  │Dispatcher:   │           │       │       │           │
  ▼      ▼       ▼           ▼       ▼       │           │
┌───┐ ┌───────────────┐ ┌──────────┐ ┌──────────────┐    │
│END│ │⑨SqlGenerateNode│ │⑫Python   │ │⑭ ReportGen  │    │
│   │ │   SQL生成      │ │GenerateNode│ eratorNode   │    │
│   │ └───────┬───────┘ │  Python代码│ │  报告生成   │    │
│   │    ┌────┴────┐    │   生成    │ └──────┬───────┘    │
│   │    │Dispatcher│    └─────┬────┘        │            │
│   │ ┌──┴──┐ ┌───┴────┐      │(固定边)     │(固定边)    │
│   │ │重试 │ │语义检查│      ▼             ▼            │
│   │ │(自环)│ │通过    │   ┌──────────────┐ ┌───┐       │
│   │ ▼     │ ▼        │   │⑬PythonExecute│ │END│       │
│   │┌──────┴──────────┐│   │   Node       │ └───┘       │
│   ││⑩ SemanticConsis- ││   │  Python执行  │             │
│   ││   tencyNode     ││   └──────┬───────┘             │
│   ││   SQL语义检查    ││    ┌─────┴──────┐              │
│   │└───────┬─────────┘│    │ Dispatcher │              │
│   │   ┌────┴────┐     │  ┌─┴──┐  ┌─────┴──┐          │
│   │   │通过     │失败  │  │成功│  │失败重试 │          │
│   │   ▼         ├─────┘  ▼    │  │(回到⑫) │          │
│   │┌────────────────────┐┌──────────────┐│             │
│   ││⑪ SqlExecuteNode   ││⑭PythonAnalyze││             │
│   ││    SQL执行         ││   Node       ││             │
│   │└─────────┬──────────┘│  Python结果  ││             │
│   │    ┌─────┴──────┐    │   分析      ││             │
│   │    │ Dispatcher │    └──────┬───────┘│             │
│   │ ┌──┴────┐┌─────┴──┐       │        │             │
│   │ │成功   ││失败    │       │(固定边) │             │
│   │ ▼       ││回⑨重试 │       └────────┘             │
│   │ └───────┘└────────┘           │                   │
│   │       │                       ▼                   │
│   │       └───────────────────────┘                   │
│   │               (回到 PlanExecutorNode 推进下一步)    │
│   │                                                   │
│   └───────────────────────────────────────────────────┘
│
│   ┌─────────────────────┐
│   │⑯ HumanFeedbackNode  │  ◀── PlanExecutorNode 路由（开启人工审核时）
│   │    人工审核          │
│   └──────────┬──────────┘
│         ┌────┴──────────┐
│         │ Dispatcher:   │
│    ┌────┴────┐  ┌───────┴───┐
│    │ 批准    │  │ 拒绝      │
│    ▼         │  ▼           │
│   回到        │ 回到⑦        │
│ PlanExecutor  │ PlannerNode  │
│ 继续执行      │ 重新生成计划 │
│               │              │
│         ┌─────┴────┐        │
│         │ 超过3次   │        │
│         ▼           │        │
│        END          │        │
└─────────────────────┘
```

**图例说明**：

| 符号 | 含义 |
|---|---|
| `──▶` | 固定边，无条件跳转到下一个节点 |
| `╌╌▶` | 条件边，由 Dispatcher 根据运行时状态动态路由 |
| `自环` | 节点路由回自身（重试机制），如 SqlGenerateNode、TableRelationNode |
| `◀──` | 回环边，执行完成后返回调度节点推进下一步 |

**三条执行主路径**：

1. **SQL 路径**：PlanExecutorNode → SqlGenerateNode → SemanticConsistencyNode → SqlExecuteNode → PlanExecutorNode
   - 语义检查失败或执行失败时，回到 SqlGenerateNode 重试（最多10次）
2. **Python 路径**：PlanExecutorNode → PythonGenerateNode → PythonExecuteNode → PythonAnalyzeNode → PlanExecutorNode
   - 执行失败时回到 PythonGenerateNode 重试（最多5次），超限则降级
3. **报告路径**：PlanExecutorNode → ReportGeneratorNode → END（固定边，最终输出）

### 3.2 节点与调度器接口

所有节点实现 `NodeAction` 接口（来自 `com.alibaba.cloud.ai.graph.action`）：

```java
public interface NodeAction {
    Map<String, Object> apply(OverAllState state) throws Exception;
}
```

- **输入**: `OverAllState` — 本质是一个线程安全的 `Map<String, Object>`
- **输出**: `Map<String, Object>` — 要更新的状态键值对

节点可以返回两种类型的值：
1. **直接值** — `Map.of("SQL_GENERATE_OUTPUT", "SELECT ...")`，立即更新状态
2. **Flux 流** — `Map.of("SQL_GENERATE_OUTPUT", flux)`，流式更新，每个 token 都会被推送到前端

所有调度器实现 `EdgeAction` 接口：

```java
public interface EdgeAction {
    String apply(OverAllState state) throws Exception;
}
```

返回下一个要执行的节点名称（字符串），或 `"END"` 表示流程结束。图的引擎会根据返回值找到对应的节点并继续执行。

---

## 第 4 站：节点逐一讲解

### 4.1 IntentRecognitionNode — 意图识别

**文件**: `workflow/node/IntentRecognitionNode.java:53-81`
**Prompt**: `resources/prompts/intent-recognition.txt`

这是整个流水线的第一个节点，充当"门卫"角色——快速判断用户输入是闲聊还是数据分析请求。

#### 工作流程

```
读取状态: input = "各省销售额排名"
    ↓
调用 PromptHelper.buildIntentRecognitionPrompt() 构建 Prompt
    ↓
调用 llmService.callUser(prompt) 获取 LLM 流式响应
    ↓
LLM 返回 JSON: {"classification": "《可能的数据分析请求》"}
    ↓
通过 FluxUtil 包装成 StreamingOutput 流
    ↓
流结束时解析 JSON，写入状态: INTENT_RECOGNITION_NODE_OUTPUT
```

#### 核心代码

```java
// IntentRecognitionNode.java:53-81
public Map<String, Object> apply(OverAllState state) throws Exception {
    // 1. 从全局状态中读取输入
    String userInput = StateUtil.getStringValue(state, INPUT_KEY);
    String multiTurn = StateUtil.getStringValue(state, MULTI_TURN_CONTEXT, "");

    // 2. 构建意图识别 Prompt
    String prompt = PromptHelper.buildIntentRecognitionPrompt(multiTurn, userInput);

    // 3. 调用 LLM（流式）
    Flux<ChatResponse> chatResponseFlux = llmService.callUser(prompt);

    // 4. 包装成流式输出，在流结束时解析结果
    return FluxUtil.createStreamingGenerator(
        INTENT_RECOGNITION_NODE,
        state,
        chatResponseFlux,
        prefixMessage("正在进行意图识别..."),   // 前缀：给用户的提示
        suffixMessage("意图识别完成！"),         // 后缀：完成提示
        collectedText -> {                       // 流结束回调
            IntentRecognitionOutputDTO result = jsonParseUtil
                .tryConvertToObject(collectedText, IntentRecognitionOutputDTO.class);
            return Map.of(INTENT_RECOGNITION_NODE_OUTPUT, result);
        }
    );
}
```

#### Prompt 模板解析

`intent-recognition.txt` 的核心设计原则是 **"宁放过，不杀错"**：

```
# 核心原则
只要用户的输入有一丝一毫的可能性是想查询或分析数据，就必须将其划分为《可能的数据分析请求》。
只有当输入明确、毫无疑问是闲聊或与数据无关时，才进行拦截。

# 分类标准
《闲聊或无关指令》:
- 纯情感/礼貌用语: "哈哈哈", "谢谢你！"
- 关于 AI 自身的元问题: "你是谁？"
- 与业务数据完全无关的常识问题: "今天天气怎么样？"

《可能的数据分析请求》:
- 包含数据分析关键词: "查询", "统计", "排名"
- 包含业务名词: "销售额", "xx部门"
- 模糊的询问: "那个呢？", "具体一点"
```

对于 "各省销售额排名"，包含"排名"关键词和"销售额"业务名词，显然被分类为"可能的数据分析请求"。

#### Dispatcher 路由

**文件**: `workflow/dispatcher/IntentRecognitionDispatcher.java:35-57`

```java
public String apply(OverAllState state) {
    IntentRecognitionOutputDTO output = StateUtil.getObjectValue(
        state, INTENT_RECOGNITION_NODE_OUTPUT, IntentRecognitionOutputDTO.class);

    if (output == null || output.getClassification() == null) {
        return END;  // 安全降级
    }
    if ("《闲聊或无关指令》".equals(output.getClassification())) {
        return END;  // 闲聊 → 结束
    }
    return EVIDENCE_RECALL_NODE;  // 数据分析 → 继续到证据召回
}
```

---

### 4.2 EvidenceRecallNode — RAG 知识检索

**文件**: `workflow/node/EvidenceRecallNode.java:62-98`
**Prompt**: `resources/prompts/evidence-query-rewrite.txt`

这一步执行 RAG（Retrieval-Augmented Generation，检索增强生成）。先改写查询，再做向量语义检索，找到相关的业务知识作为后续节点的"证据"。

> **RAG 是什么？** 大模型的知识来自训练数据，不知道你企业的业务术语和规则。RAG 的思路是：先把企业知识存入向量数据库，用户提问时先检索相关知识，再把检索结果连同用户问题一起喂给大模型。这样大模型就能基于企业知识来回答。

#### 工作流程

```
Phase 1 — 查询改写:
  "各省销售额排名" → LLM → "查询各省/自治区的销售额排名"

Phase 2 — 向量检索:
  用改写后的查询 → VectorStore 语义搜索:
    ① 搜索 business_knowledge（业务术语向量）
    ② 搜索 agent_knowledge（知识库文档向量）
  → 返回匹配的文档片段

Phase 3 — 格式化:
  FAQ/QA 类型: "[来源: 标题] Q: ... A: ..."
  文档类型:   "[来源: 标题-文件名] 内容..."
  → 拼接成 evidence 字符串
```

#### 核心代码

```java
// EvidenceRecallNode.java:62-98
public Map<String, Object> apply(OverAllState state) {
    String question = StateUtil.getStringValue(state, INPUT_KEY);
    String agentId = StateUtil.getStringValue(state, AGENT_ID);
    String multiTurn = StateUtil.getStringValue(state, MULTI_TURN_CONTEXT, "");

    // Phase 1: 查询改写 — 把口语化的查询转为标准的检索语句
    String rewritePrompt = PromptHelper.buildEvidenceQueryRewritePrompt(multiTurn, question);
    Flux<ChatResponse> rewriteFlux = llmService.callUser(rewritePrompt);

    return FluxUtil.cascadeFlux(rewriteFlux,
        // Phase 2: 用改写后的查询做向量检索
        collectedText -> {
            String standaloneQuery = extractStandaloneQuery(collectedText);
            return getEvidences(agentId, standaloneQuery);
        },
        // 聚合函数：把检索结果转为 evidence 字符串
        collectedText -> collectedText,
        // 前缀消息
        prefixMessage("正在检索相关知识..."),
        // 中间消息
        middleMessage("相关知识检索完成，正在整理..."),
        // 后缀消息
        suffixMessage("知识检索完成！")
    );
}
```

**cascadeFlux** 是 FluxUtil 提供的级联操作——第一个 Flux（查询改写）的结果作为第二个操作（向量检索）的输入。

#### 向量检索细节

```java
// EvidenceRecallNode.java:151-176
private String getEvidences(String agentId, String query) {
    // 搜索业务术语
    List<Document> bizDocs = vectorStoreService.getDocumentsForAgent(
        agentId, query,
        DocumentMetadataConstant.BUSINESS_TERM,  // 向量类型：业务术语
        topK, threshold                           // TopK 和相似度阈值
    );

    // 搜索 Agent 知识库
    List<Document> knowledgeDocs = vectorStoreService.getDocumentsForAgent(
        agentId, query,
        DocumentMetadataConstant.AGENT_KNOWLEDGE, // 向量类型：知识库
        topK, threshold
    );

    // 格式化输出
    return formatEvidence(bizDocs, knowledgeDocs);
}
```

---

### 4.3 QueryEnhanceNode — 查询增强

**文件**: `workflow/node/QueryEnhanceNode.java:50-75`
**Prompt**: `resources/prompts/query-enhancement.txt`

这一步对用户查询做深度优化：解析代词、转换相对时间、替换业务术语、生成查询变体。

#### 工作流程

```
输入: "各省销售额排名" + evidence（上一步检索的业务知识）
    ↓
LLM 增强（三步走）:
  1. 澄清 & 规范化: 解析代词、时间转绝对日期、术语替换
  2. 扩展: 生成 2-3 个语义相同的变体查询
    ↓
输出 QueryEnhanceOutputDTO:
  canonical_query: "查询各省份/直辖市/自治区的销售额总量，按降序排列"
  expanded_queries: ["各省市销售总额排行", "按省份统计销售额并排序"]
```

#### Prompt 模板关键部分

```
query-enhancement.txt 的三步走：

1. 查询澄清与规范化 (Clarify & Normalize):
   - 指代消解: "它" → 具体名词
   - 时间转换: "上个月" → "2025-10-01至2025-10-31"
   - 业务术语解析: "核心用户" → "消费总额超过5000元的用户"（基于 evidence）

2. 查询扩展 (Expand):
   - 生成 2-3 个语义相同但表达不同的扩展问题
```

**输出 DTO**:

```java
// dto/prompt/QueryEnhanceOutputDTO.java
public class QueryEnhanceOutputDTO {
    @JsonProperty("canonical_query")      // 规范化查询
    private String canonicalQuery;

    @JsonProperty("expanded_queries")     // 扩展查询列表
    private List<String> expandedQueries;
}
```

`canonicalQuery` 会被后续所有节点使用，是整个工作流的"标准查询"。`expandedQueries` 用于扩大检索范围（多几个搜索词，命中率更高）。

---

### 4.4 SchemaRecallNode — 表结构召回

**文件**: `workflow/node/SchemaRecallNode.java:62-137`

这个节点 **不调用 LLM**，纯粹基于向量检索。用增强后的查询去找相关的数据库表和列。

#### 工作流程

```
读取: canonicalQuery = "查询各省份/自治区的销售额总量，按降序排列"
    ↓
查找 Agent 绑定的活跃数据源:
  agentDatasourceMapper.selectActiveDatasourceIdByAgentId(agentId)
    ↓
表级向量检索:
  schemaService.getTableDocumentsByDatasource(datasourceId, canonicalQuery)
  → 找到相关表: orders, provinces
    ↓
列级向量检索（基于找到的表名）:
  schemaService.getColumnDocumentsByTableName(datasourceId, recalledTableNames)
  → 找到相关列: orders.amount, orders.province_id, provinces.name
    ↓
写入状态:
  TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT = [表文档列表]
  COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT = [列文档列表]
```

#### 关键代码

```java
// SchemaRecallNode.java:102-106
List<Document> tableDocs = schemaService.getTableDocumentsByDatasource(datasourceId, input);
List<String> recalledTableNames = extractTableNames(tableDocs);
List<Document> columnDocs = schemaService.getColumnDocumentsByTableName(datasourceId, recalledTableNames);
```

**如果没有找到任何表**，节点会返回一个错误消息，后续的 SchemaRecallDispatcher 会路由到 END，终止流程。这种情况通常发生在：
1. 数据源未初始化（没有向量化表结构）
2. 用户查询与数据源中的表完全不相关
3. Embedding 模型不匹配（换了模型后需要重新向量化）

---

### 4.5 TableRelationNode — 构建完整 Schema

**文件**: `workflow/node/TableRelationNode.java:94-159`

这一步把上一步召回的表和列组装成完整的 `SchemaDTO`，然后调用 LLM 做"精选"——从几十个列中只保留查询需要的，再读取语义模型映射。

#### 工作流程

```
Phase 1 — 组装初始 SchemaDTO:
  读取表/列文档 → schemaService.buildSchemaFromDocuments()
  读取逻辑外键（logical_relation 表）→ 加入 foreignKeys
  ↓
  SchemaDTO {
    name: "product_db",
    tables: [
      TableDTO { name: "orders", columns: [50个列...] },
      TableDTO { name: "provinces", columns: [5个列...] }
    ],
    foreignKeys: ["orders.province_id=provinces.id"]
  }

Phase 2 — LLM 精选:
  输入: 完整 SchemaDTO + 用户查询
  LLM: "用户只查省份和销售额，去掉其他不相关的列"
  输出: 精简后的 SchemaDTO（只保留 amount, province_id, name 列）

Phase 3 — 语义模型:
  读取 semantic_model 表 → 生成字段语义描述
  例如: amt → "销售金额", province_id → "省份编码"
    ↓
写入状态: TABLE_RELATION_OUTPUT = 精简后的 SchemaDTO
```

#### SchemaDTO 结构

```java
// dto/schema/SchemaDTO.java
public class SchemaDTO {
    private String name;              // 数据库名
    private String description;       // 数据库描述
    private Integer tableCount;       // 表数量
    private List<TableDTO> table;     // 表列表
    private List<String> foreignKeys; // 外键关系 ["orders.province_id=provinces.id"]
}

// dto/schema/TableDTO.java
public class TableDTO {
    private String name;              // 表名
    private String description;       // 表描述
    private List<ColumnDTO> columns;  // 列列表
}

// dto/schema/ColumnDTO.java
public class ColumnDTO {
    private String name;              // 列名
    private String type;              // 数据类型
    private String comment;           // 注释/描述
    private boolean isPrimaryKey;     // 是否主键
    private List<String> examples;    // 示例值（最多3个）
}
```

#### LLM 精选的 Prompt

精选使用 `mix-selector.txt` 模板，它是一个经典的 few-shot（少样本）Prompt，提供了两个完整示例，然后让 LLM 根据用户问题和 Schema 选择相关表和列。

---

### 4.6 FeasibilityAssessmentNode — 可行性评估

**文件**: `workflow/node/FeasibilityAssessmentNode.java:46-75`
**Prompt**: `resources/prompts/feasibility-assessment.txt`

LLM 综合评估：现有 Schema + Evidence 能不能回答用户的问题。

#### 工作流程

```
输入: canonicalQuery + SchemaDTO + evidence + 多轮上下文
    ↓
LLM 判断（三种输出之一）:

【需求类型】：《数据分析》       → 可以回答，继续
【需求类型】：《需要澄清》       → 关键信息缺失，终止并反问用户
【需求类型】：《自由闲聊》       → 与数据无关，终止
```

#### Prompt 核心原则

```
feasibility-assessment.txt 的判断逻辑：

乐观原则: 只要在 Schema 和 Evidence 中能找到相关线索，就优先尝试数据分析。
只有在遇到无法绕过的关键信息缺失时，才请求澄清。

《需要澄清》的触发条件（严格）:
- 核心实体在 Schema 和 Evidence 中都找不到任何对应
- 查询中的决定性概念非常模糊，且 Evidence 没有给出定义
```

对于 "各省销售额排名"，Schema 中有 orders 表和省份字段 → "数据分析" → 继续。

#### Dispatcher 路由

```java
// FeasibilityAssessmentDispatcher.java:37-43
if (output.contains("【需求类型】：《数据分析》")) {
    return PLANNER_NODE;
}
return END;
```

---

### 4.7 PlannerNode — 生成执行计划

**文件**: `workflow/node/PlannerNode.java:53-68`
**Prompt**: `resources/prompts/planner.txt`（154行，是所有 Prompt 中最复杂的）

这是最关键的节点之一——LLM 把用户需求拆解成一个多步执行计划（Plan），每一步指定使用什么工具（SQL / Python / 报告生成）。

#### 工作流程

```
输入: canonicalQuery + SchemaDTO + evidence + 语义模型
    ↓
LLM 生成 Plan（JSON 格式）:
{
  "thought_process": "用户需要各省份销售额排名，需要从 orders 表按省份聚合...",
  "execution_plan": [
    {
      "step": 1,
      "tool_to_use": "SQL_GENERATE_NODE",
      "tool_parameters": {
        "instruction": "按省份分组汇总销售额，降序排列，取前10"
      }
    },
    {
      "step": 2,
      "tool_to_use": "REPORT_GENERATOR_NODE",
      "tool_parameters": {
        "summary_and_recommendations": "生成各省份销售额排名的柱状图报告，标注Top3省份"
      }
    }
  ]
}
    ↓
写入状态: PLANNER_NODE_OUTPUT = 上面的 JSON 字符串
```

#### Plan 数据结构

```java
// dto/planner/Plan.java
public class Plan {
    @JsonProperty("thought_process")
    private String thoughtProcess;          // LLM 的推理过程

    @JsonProperty("execution_plan")
    private List<ExecutionStep> executionPlan;  // 有序执行步骤
}

// dto/planner/ExecutionStep.java
public class ExecutionStep {
    @JsonProperty("step")
    private int step;                       // 步骤序号

    @JsonProperty("tool_to_use")
    private String toolToUse;               // "SQL_GENERATE_NODE" / "PYTHON_GENERATE_NODE" / "REPORT_GENERATOR_NODE"

    @JsonProperty("tool_parameters")
    private ToolParameters toolParameters;  // 工具参数
}

// dto/planner/ToolParameters.java
public class ToolParameters {
    @JsonProperty("instruction")
    private String instruction;             // SQL/Python 的具体指令

    @JsonProperty("summary_and_recommendations")
    private String summaryAndRecommendations; // 报告大纲

    @JsonProperty("sql_query")
    private String sqlQuery;               // 运行时填充（SQL执行后回写）
}
```

#### Prompt 的三种工具说明

```
planner.txt 定义了三种可用工具：

SQL_GENERATE_NODE:
  - 用途: 执行 SQL 查询提取或聚合数据
  - 参数: instruction（给下游 SQL 生成节点的 Prompt）
  - 约束: instruction 必须详细，包含表名、维度、过滤条件
  - 反例: "SQL生成"（绝对禁止）
  - 正例: "从 orders 表查询 2023 年北京地区的销售总额，按月份分组"

PYTHON_GENERATE_NODE:
  - 用途: SQL 难以满足的复杂计算、统计分析、图表绘制
  - 参数: instruction（给 Python 解释器的编程指令）

REPORT_GENERATOR_NODE:
  - 用途: 流程的最后一步，整合所有步骤输出生成报告
  - 参数: summary_and_recommendations（报告大纲和建议方向）
  - 约束: 必须是计划的最后一步
```

#### NL2SQL 模式

当 `IS_ONLY_NL2SQL = true` 时，PlannerNode 跳过 LLM 调用，直接返回一个硬编码的单步计划：

```java
// PlannerNode.java:106-108
return Map.of(PLANNER_NODE_OUTPUT, """
    {"thought_process":"NL2SQL模式","execution_plan":[
      {"step":1,"tool_to_use":"SQL_GENERATE_NODE","tool_parameters":{"instruction":"%s"}}
    ]}
    """.formatted(canonicalQuery));
```

#### 计划修复（Retry）

当 PlanExecutorNode 验证计划失败（JSON 格式错误、步骤为空、工具名无效），会在状态中设置 `PLAN_VALIDATION_ERROR`。PlannerNode 下次被调用时，会把这个错误信息加入 Prompt，让 LLM 重新生成：

```java
// PlannerNode.java:76-82
String planValidationError = StateUtil.getStringValue(state, PLAN_VALIDATION_ERROR, "");
if (StringUtils.hasText(planValidationError)) {
    // 重试模式：把错误信息加入 Prompt
    params.put("plan_validation_error", planValidationError);
}
```

---

### 4.8 PlanExecutorNode — 计划执行调度器

**文件**: `workflow/node/PlanExecutorNode.java:50-101`

这个节点本身 **不执行任何业务逻辑**，只做两件事：**验证计划** 和 **推进步骤**。它就像一个项目经理——不亲自干活，但负责分配任务和跟踪进度。

#### 工作流程

```
每次进入 PlanExecutorNode:

1. 解析并验证 Plan（JSON 格式、步骤非空、工具名有效）
   ↓ 验证失败？
   → 设置 PLAN_VALIDATION_STATUS=false，PLAN_VALIDATION_ERROR=错误信息
   → PlanExecutorDispatcher 路由回 PLANNER_NODE 重新生成（最多2次）

2. 检查人工审核开关
   ↓ HUMAN_REVIEW_ENABLED=true？
   → 路由到 HUMAN_FEEDBACK_NODE，图暂停等待人工审核

3. 推进步骤
   currentStep = state.get(PLAN_CURRENT_STEP, 1)
   ↓
   executionStep = plan.get(currentStep - 1)
   ↓
   PLAN_NEXT_NODE = executionStep.toolToUse
   → PlanExecutorDispatcher 路由到对应的工具节点
```

#### 步骤推进示例

```
第 1 次进入: currentStep=1, plan[0].toolToUse="SQL_GENERATE_NODE"
  → 路由到 SqlGenerateNode

SQL 执行完回来: currentStep=2（SqlExecuteNode 在成功后 currentStep++）
  plan[1].toolToUse="REPORT_GENERATOR_NODE"
  → 路由到 ReportGeneratorNode

报告生成完回来: currentStep=3 > plan.size()=2
  → 所有步骤完成，路由到 REPORT_GENERATOR_NODE（最终报告）或 END
```

#### Dispatcher 路由

```java
// PlanExecutorDispatcher.java:38-62
if (planValidationStatus == true) {
    return state.get(PLAN_NEXT_NODE);  // 路由到 SQL/Python/报告节点
}
if (planRepairCount > 2) {
    return END;  // 修复超过2次，放弃
}
return PLANNER_NODE;  // 路由回计划器重新生成
```

---

### 4.9 SqlGenerateNode — SQL 生成

**文件**: `workflow/node/SqlGenerateNode.java:68-153`
**Prompt**: `resources/prompts/new-sql-generate.txt`（首次生成）/ `resources/prompts/sql-error-fixer.txt`（错误修复）

LLM 根据表结构和执行步骤描述生成 SQL。

#### 工作流程

```
读取状态:
  SQL_GENERATE_COUNT = 0（首次）/ N（重试）
  SQL_REGENERATE_REASON = 空（首次）/ 重试原因
  TABLE_RELATION_OUTPUT = SchemaDTO
  EVIDENCE = 业务知识
  DB_DIALECT_TYPE = "MySQL"
  PLAN_CURRENT_STEP → 当前步骤的 instruction
    ↓
判断生成模式:
  - 首次 → buildNewSqlGeneratorPrompt()
  - 语义检查失败 → buildSqlErrorFixerPrompt（带失败原因）
  - SQL 执行失败 → buildSqlErrorFixerPrompt（带错误信息）
    ↓
调用 LLM 生成 SQL:
  SELECT p.name AS province, SUM(o.amount) AS total_sales
  FROM orders o JOIN provinces p ON o.province_id = p.id
  GROUP BY p.name ORDER BY total_sales DESC
    ↓
写入状态: SQL_GENERATE_OUTPUT = 上面的 SQL
```

#### 重试机制详解

```java
// SqlGenerateNode.java:68-81 — 重试计数守卫
int sqlGenerateCount = StateUtil.getObjectValue(state, SQL_GENERATE_COUNT, Integer.class, 0);
if (sqlGenerateCount >= properties.getMaxSqlRetryCount()) {
    // 超过最大重试次数（默认10次），写入 END 信号终止
    return Map.of(SQL_GENERATE_OUTPUT, StateGraph.END);
}

// SqlGenerateNode.java:89-106 — 根据重试原因选择模式
SqlRetryDto retryDto = StateUtil.getObjectValue(state, SQL_REGENERATE_REASON, SqlRetryDto.class, SqlRetryDto.empty());

if (retryDto.sqlExecuteFail()) {
    // SQL 执行失败 → 修复模式（带上错误信息）
    handleRetryGenerateSql(state, existingSql, retryDto.exceptionMessage(), instruction);
} else if (retryDto.semanticFail()) {
    // 语义检查失败 → 修复模式（带上不通过原因）
    handleRetryGenerateSql(state, existingSql, retryDto.exceptionMessage(), instruction);
} else {
    // 首次生成
    handleGenerateSql(state, instruction);
}
```

#### SqlRetryDto — 重试信息载体

```java
// dto/datasource/SqlRetryDto.java
public record SqlRetryDto(
    boolean sqlExecuteFail,     // SQL 执行是否失败
    boolean semanticFail,       // 语义检查是否失败
    String exceptionMessage     // 错误/失败原因
) {
    public static SqlRetryDto empty()          { return new SqlRetryDto(false, false, ""); }
    public static SqlRetryDto semantic(String msg)  { return new SqlRetryDto(false, true, msg); }
    public static SqlRetryDto sqlExecute(String msg) { return new SqlRetryDto(true, false, msg); }
}
```

这个 DTO 是 SQL 重试循环中的关键数据结构，线程化地传递失败类型和原因。

#### SQL 生成 Prompt 的关键约束

`new-sql-generate.txt` 的核心设计：

```
# 核心约束
1. 方言兼容: 严格遵循 MySQL/PostgreSQL/Oracle/SQL Server/Hive 的语法规范
   - MySQL: 反引号 `order`
   - PostgreSQL: 双引号 "order"
   - SQL Server: 方括号 [order]
   - Hive: 不用引号

2. 结果集控制: 不要 SELECT *，只选需要的列

3. 格式规范: 只输出 SQL，不要 Markdown 标记，不要注释

4. 安全转义: 对所有表名和列名加引号避免保留字冲突

5. Hive 特殊规则: 不加分号，不用反引号
```

#### Dispatcher 路由

```java
// SqlGenerateDispatcher.java:41-64
if (SQL_GENERATE_OUTPUT 为空) {
    if (SQL_GENERATE_COUNT < maxSqlRetryCount) {
        return SQL_GENERATE_NODE;  // 重试
    }
    return END;  // 超过次数，终止
}
if (SQL_GENERATE_OUTPUT == "END") {
    return END;  // 节点主动终止
}
return SEMANTIC_CONSITY_NODE;  // SQL 生成成功 → 语义检查
```

---

### 4.10 SemanticConsistencyNode — SQL 语义检查

**文件**: `workflow/node/SemanticConsistencyNode.java:57-87`
**Prompt**: `resources/prompts/semantic-consistency.txt`

让另一个 LLM 做"Code Review"——审查生成的 SQL 是否正确。这是质量控制环节。

#### 工作流程

```
输入: SQL + SchemaDTO + 执行步骤描述 + evidence + 用户查询
    ↓
LLM 审查（两个维度）:
  维度1 — 语义一致性: SQL 是否完成了步骤指令要求的任务？
  维度2 — 结构正确性: 所有表名/列名是否在 Schema 中存在？方言是否正确？
    ↓
输出:
  "通过" → 继续
  "不通过。GROUP BY 缺少 province_name 列" → 回到 SqlGenerateNode 重试
```

#### Prompt 审计维度

```
semantic-consistency.txt 的两个审计维度：

维度 1：语义一致性 (Semantic)
- 目标对齐: SQL 是否查询了执行指令中要求的表和字段？
- 过滤准确: 是否遗漏了指令中的 WHERE 条件？
- 聚合正确: GROUP BY 是否符合指令意图？

维度 2：结构正确性 (Syntax & Schema)
- 幻觉检测: 所有表名/列名是否在 Schema 中存在？
- 方言兼容: 是否使用了正确的数据库方言？

通过条件（宽松）:
- 逻辑正确，字段存在
- 非核心的排序差异（允许）
- 多余的 ID 列（允许，通常用于后续 Join）
```

#### Dispatcher 路由

```java
// SemanticConsistenceDispatcher.java:31-42
Boolean passed = StateUtil.getObjectValue(state, SEMANTIC_CONSISTENCY_NODE_OUTPUT, Boolean.class, false);
if (passed) {
    return SQL_EXECUTE_NODE;      // 通过 → 执行 SQL
} else {
    return SQL_GENERATE_NODE;     // 不通过 → 重新生成
}
```

---

### 4.11 SqlExecuteNode — SQL 执行

**文件**: `workflow/node/SqlExecuteNode.java:89-269`

真正执行 SQL 并获取结果。这是工作流中唯一直接操作数据库的节点。

#### 工作流程

```
1. 获取数据库连接:
   agentId → agentDatasourceMapper → datasource 表 → DbConfigBO
   → AccessorFactory.getAccessor(dbConfig) → MySQLAccessor
   → 从 Druid 连接池获取 JDBC Connection

2. 执行 SQL:
   SqlExecutor.executeSqlAndReturnObject()
   → statement.setMaxRows(1000)     // 硬编码，最多返回1000行
   → statement.setQueryTimeout(30)  // 硬编码，30秒超时
   → ResultSetBuilder.buildFrom(rs)
   → ResultSetBO { columns: [...], data: [{...}, ...] }

3. 图表推荐（可选，默认开启）:
   取前20行样本数据 → 调用 LLM → 推荐图表类型
   对于 "各省销售额排名" → 推荐"柱状图"（分类对比场景）
   超时: 3000ms

4. 存储结果:
   SQL_RESULT_LIST_MEMORY = 数据行列表（供后续 Python 使用）
   SQL_EXECUTE_NODE_OUTPUT = JSON 字符串（key 为 "step_1"）
   PLAN_CURRENT_STEP = currentStep + 1（推进到下一步）
   SQL_GENERATE_COUNT = 0（重置重试计数）
```

#### 图表推荐细节

```java
// SqlExecuteNode.java:208-267
private void enrichResultSetWithChartConfig(...) {
    if (!properties.isEnableSqlResultChart()) return;

    // 取前20行样本
    List<Map<String, String>> sampleData = resultSetBO.getData()
        .stream().limit(SAMPLE_DATA_NUMBER).toList();

    // 调用 LLM 推荐图表类型
    String prompt = PromptHelper.buildDataViewAnalysisPrompt();
    // prompt 包含: 支持的图表类型(table/column/bar/line/pie)
    //              选择原则: 趋势→line, 分类对比→column/bar, 占比→pie

    // 3秒超时
    DisplayStyleBO chartConfig = llmService.callUser(prompt)
        .block(Duration.ofMillis(properties.getEnrichSqlResultTimeout()));

    resultSetBO.setDisplayStyle(chartConfig);
}
```

#### SQL 执行失败处理

```java
// SqlExecuteNode.java:185-190
catch (Exception e) {
    result.put(SQL_REGENERATE_REASON, SqlRetryDto.sqlExecute(e.getMessage()));
    // Dispatcher 会路由回 SqlGenerateNode，带上错误信息
}
```

#### Dispatcher 路由

```java
// SQLExecutorDispatcher.java:33-43
SqlRetryDto retryDto = StateUtil.getObjectValue(state, SQL_REGENERATE_REASON, ...);
if (retryDto.sqlExecuteFail()) {
    return SQL_GENERATE_NODE;    // 执行失败 → 重新生成 SQL
}
return PLAN_EXECUTOR_NODE;       // 执行成功 → 回到计划执行器推进下一步
```

#### SqlExecutor 的硬限制

```java
// connector/SqlExecutor.java:35-37
private static final int RESULT_SET_LIMIT = 1000;    // 最多1000行
private static final int STATEMENT_TIMEOUT = 30;      // 30秒超时

// connector/ResultSetBuilder.java:45
while (rs.next() && count < SqlExecutor.RESULT_SET_LIMIT)  // 双重保险
```

---

### 4.12 PythonGenerateNode — Python 代码生成

**文件**: `workflow/node/PythonGenerateNode.java:50-131`
**Prompt**: `resources/prompts/python-generator.txt`（161行，最长的 Prompt 之一）

当执行计划中包含 `PYTHON_GENERATE_NODE` 步骤时，LLM 生成 Python 代码用于复杂数据分析（SQL 做不了的统计、计算、可视化）。

#### 工作流程

```
读取状态:
  TABLE_RELATION_OUTPUT = SchemaDTO（表结构）
  SQL_RESULT_LIST_MEMORY = SQL 查询结果（取前5行作为样本）
  PLAN_CURRENT_STEP → 当前步骤的 instruction
  PYTHON_TRIES_COUNT = 重试计数
    ↓
构建 Prompt:
  System Prompt:
    - 角色: 专业 Python 数据分析工程师
    - 输入: 从 sys.stdin 读取 JSON
    - 输出: 通过 print(json.dumps(result)) 输出 JSON
    - 约束: 只用 Anaconda 默认库，禁止文件/网络操作，500MB 内存上限
    - 表结构（JSON）
    - 样本数据（前5行）

  User Prompt:
    - 用户查询
    - 重试时: 追加上次失败的代码和错误信息
    ↓
调用 LLM 生成 Python 代码
    ↓
清洗输出（去掉 Markdown 代码块标记）
    ↓
写入状态: PYTHON_GENERATE_NODE_OUTPUT = Python 代码
```

#### Prompt 的代码模板

`python-generator.txt` 提供了一个标准代码骨架，LLM 在此基础上填充逻辑：

```python
import sys
import json
import traceback
import pandas as pd

try:
    # 从 stdin 读取输入数据
    input_data = json.load(sys.stdin)
    df = pd.DataFrame(input_data)

    # 自动类型推断（后端统一返回字符串，需要转数值）
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')

    # ... 分析逻辑 ...

    print(json.dumps(result, ensure_ascii=False))
except Exception:
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
```

#### 重试逻辑

```java
// PythonGenerateNode.java:80-97
if (!codeRunSuccess && triesCount > 0) {
    // 重试：追加上次失败的代码和错误
    userPrompt += "上次生成的代码执行失败:\n"
        + "代码: " + previousCode + "\n"
        + "错误: " + previousError;
}
```

---

### 4.13 PythonExecuteNode — Python 代码执行

**文件**: `workflow/node/PythonExecuteNode.java:74-167`

在沙箱环境中执行 LLM 生成的 Python 代码。

#### 工作流程

```
读取: PYTHON_GENERATE_NODE_OUTPUT = Python 代码
      SQL_RESULT_LIST_MEMORY = SQL 结果（全部，最多1000行）
    ↓
构建 TaskRequest:
  code = Python 代码
  input = SQL 结果的 JSON 序列化（通过 stdin 传给 Python）
    ↓
调用 CodePoolExecutorService.runTask():
  Docker 模式: 在容器中执行 python3 -u script.py < input_data.txt
  Local 模式:  在本地进程中执行
    ↓
处理结果:
  成功: PYTHON_IS_SUCCESS=true, PYTHON_EXECUTE_NODE_OUTPUT=stdout JSON
  失败 + 超过最大重试: PYTHON_FALLBACK_MODE=true（降级模式）
  失败 + 还有重试机会: PYTHON_IS_SUCCESS=false
```

#### 数据传递机制

```java
// PythonExecuteNode.java:74-81
String pythonCode = StateUtil.getStringValue(state, PYTHON_GENERATE_NODE_OUTPUT);
List<Map<String, String>> sqlResults = StateUtil.getListValue(state, SQL_RESULT_LIST_MEMORY);

CodePoolExecutorService.TaskRequest taskRequest = new TaskRequest(
    pythonCode,
    objectMapper.writeValueAsString(sqlResults),  // 全部 SQL 结果序列化为 JSON
    null  // pip packages（当前未使用）
);
```

Python 进程通过 **stdin** 接收数据（不是文件、不是 HTTP），通过 **stdout** 输出结果 JSON。stderr 用于错误输出。

#### 容错机制

```java
// PythonExecuteNode.java:91-108
if (executionFailed && triesCount >= pythonMaxTriesCount) {
    // 超过最大重试次数，进入降级模式
    result.put(PYTHON_EXECUTE_NODE_OUTPUT, "{}");
    result.put(PYTHON_IS_SUCCESS, false);
    result.put(PYTHON_FALLBACK_MODE, true);  // 标记降级
}
```

#### Dispatcher 路由

```java
// PythonExecutorDispatcher.java:41-65
if (fallbackMode) return PYTHON_ANALYZE_NODE;   // 降级 → 直接分析（输出降级消息）
if (!success && tries < maxTries) return PYTHON_GENERATE_NODE;  // 重试
if (!success && tries >= maxTries) return END;                   // 放弃
return PYTHON_ANALYZE_NODE;                      // 成功 → 分析结果
```

---

### 4.14 PythonAnalyzeNode — Python 结果分析

**文件**: `workflow/node/PythonAnalyzeNode.java:56-101`
**Prompt**: `resources/prompts/python-analyze.txt`

LLM 把 Python 执行的 JSON 输出翻译成自然语言摘要。

#### 工作流程

```
读取: PYTHON_EXECUTE_NODE_OUTPUT = Python stdout
      PLAN_CURRENT_STEP = 当前步骤
      SQL_EXECUTE_NODE_OUTPUT = 已有的步骤结果
    ↓
降级模式？
  是 → 返回 "Python 高级分析功能暂时不可用，出现错误"
  否 → LLM 分析:
    输入: Python 输出 + 用户原始查询
    输出: 自然语言总结
    ↓
写入状态:
  SQL_EXECUTE_NODE_OUTPUT.put("step_N_analysis", 分析文本)
  PLAN_CURRENT_STEP++（推进到下一步）
```

#### 降级模式

当 Python 执行反复失败且超过最大重试次数时，系统不会直接报错终止，而是进入降级模式——跳过 Python 分析，返回一个固定的降级消息。这保证了即使 Python 不可用，工作流仍能继续执行后续步骤（如报告生成）。

---

### 4.15 ReportGeneratorNode — 报告生成

**文件**: `workflow/node/ReportGeneratorNode.java:76-237`
**Prompt**: `resources/prompts/report-generator-plain.txt`

工作流的最后一个节点。把所有步骤的结果组装成一份 Markdown 格式的分析报告。

#### 工作流程

```
读取:
  PLANNER_NODE_OUTPUT = 执行计划 JSON
  SQL_EXECUTE_NODE_OUTPUT = 所有步骤结果 Map
    "step_1": SQL 结果 JSON
    "step_1_analysis": Python 分析文本（如果有）
    "step_2": ...
  PLAN_CURRENT_STEP → 当前步骤的 summary_and_recommendations
  AGENT_ID → 用户自定义的 Prompt 优化配置
    ↓
构建报告 Prompt（三部分）:
  Part 1 - 用户需求与计划:
    用户原始查询 + Plan 的 thought_process + 每步的工具和指令

  Part 2 - 分析步骤与数据:
    每步的: 步骤号、工具名、指令、执行的 SQL、原始数据、分析结论

  Part 3 - 总结与推荐:
    Planner 给出的 summary_and_recommendations

  Part 4 - 用户自定义优化指令（最高优先级）:
    从 user_prompt_config 表加载的 per-agent 自定义配置
    ↓
调用 LLM 生成 Markdown 报告
    ↓
写入状态:
  RESULT = 报告内容（Markdown 格式，可包含 ECharts 图表）
  SQL_EXECUTE_NODE_OUTPUT = null（清理）
  PLAN_CURRENT_STEP = null（清理）
  PLANNER_NODE_OUTPUT = null（清理）
    ↓
→ END（流程结束）
```

#### ECharts 图表嵌入

报告支持内嵌 ECharts 图表。LLM 在 Markdown 中使用特殊的代码块：

````markdown
```echarts
{
  "xAxis": {"type": "category", "data": ["广东","江苏","浙江"]},
  "yAxis": {"type": "value"},
  "series": [{"type": "bar", "data": [15230, 12450, 9800]}]
}
```
````

前端识别 ` ```echarts ` 代码块后，调用 ECharts 库渲染图表。

#### 用户自定义优化指令

```java
// ReportGeneratorNode.java:89-98
List<UserPromptConfig> optimizationConfigs =
    promptConfigService.getOptimizationConfigs("report-generator", agentId);
```

每个 Agent 可以配置自己的 Prompt 优化指令（如"报告必须包含同比分析"、"用英文输出"），这些指令会被注入到报告 Prompt 中，标记为"最高优先级"。

---

### 4.16 HumanFeedbackNode — 人工审核

**文件**: `workflow/node/HumanFeedbackNode.java:44-86`

当 Agent 开启了 `HUMAN_REVIEW_ENABLED` 配置时，PlanExecutorNode 不会直接执行计划，而是路由到这个节点。图会在执行到这里之前暂停（通过 `compiledGraph` 的 `interruptBefore` 配置），等待人工审核计划。

#### 工作流程

```
图暂停，等待用户在前端看到计划并做出决定:
    ↓
用户选择:
  批准 → handleHumanFeedback() 恢复图执行
  拒绝 → 带上反馈内容恢复图执行

HumanFeedbackNode 处理:
  批准: human_next_node = PLAN_EXECUTOR_NODE → 继续执行计划
  拒绝: human_next_node = PLANNER_NODE
         PLAN_REPAIR_COUNT++
         PLAN_VALIDATION_ERROR = 用户反馈
         PLANNER_NODE_OUTPUT = ""（清空，重新生成）
         PLAN_CURRENT_STEP = 1（重置）
```

#### 最大修复次数

```java
// HumanFeedbackNode.java:44-49
if (planRepairCount >= 3) {
    // 最多拒绝3次，超过后终止
    return Map.of(HUMAN_NEXT_NODE, "END");
}
```

---

## 第 5 站：结果流回前端

### 5.1 流式推送机制

**文件**: `GraphServiceImpl.java:287-343`

每个节点内部调用 LLM 时，使用的是流式 API（`Flux<ChatResponse>`），每个 token（词元）都会被包装并推送到前端。

```
LLM 流式输出（逐 token）:
  "SELECT" → " p" → ".name" → " FROM" → ...
      ↓
FluxUtil.createStreamingGenerator() 包装:
  每个 ChatResponse → StreamingOutput(nodeName, text, state)
  流结束 → GraphResponse.done(resultMap)
      ↓
GraphServiceImpl.handleStreamNodeOutput():
  检测 TextType（通过特殊标记符号）:
    "```json" → textType = JSON
    "```sql"  → textType = SQL
    "```python" → textType = PYTHON
    Markdown 标记 → textType = MARK_DOWN
      ↓
  包装成 GraphNodeResponse:
    {nodeName: "SqlGenerateNode", textType: "SQL", text: "SELECT p.name..."}
      ↓
  sink.tryEmitNext(ServerSentEvent.builder(response).build())
      ↓
  Spring WebFlux 自动转成 SSE HTTP 响应:
    data: {"nodeName":"SqlGenerateNode","textType":"SQL","text":"SELECT p.name..."}
```

### 5.2 FluxUtil — 流式粘合层

**文件**: `util/FluxUtil.java`

FluxUtil 是连接 LLM 流式输出和图引擎流式输出的桥梁。核心方法：

| 方法 | 用途 |
|---|---|
| `createStreamingGenerator()` | 包装 LLM Flux，添加前缀/后缀消息，流结束时解析结果 |
| `createStreamingGeneratorWithMessages()` | 同上，用字符串消息代替 Flux 前缀/后缀 |
| `cascadeFlux()` | 级联两个 Flux：第一个完成后，用其结果触发第二个操作 |
| `toStreamingResponseFlux()` | 底层转换：`ChatResponse` → `GraphResponse<StreamingOutput>` |

每个方法还会自动提取 token 使用量，上报给 Langfuse 可观测性系统。

### 5.3 前端实时渲染

**文件**: `data-agent-frontend/src/views/AgentRun.vue:685-819`

前端的 `onMessage` 回调根据 `textType` 分发渲染：

```javascript
// AgentRun.vue:685-819
const onMessage = (response: GraphNodeResponse) => {
    // 追踪当前执行的节点名
    currentNodeName.value = response.nodeName;

    // 根据 textType 分发渲染
    switch (response.textType) {
        case 'SQL':
            // SQL 代码块 — 语法高亮显示
        case 'JSON':
            // 结构化数据 — 表格展示
        case 'PYTHON':
            // Python 代码块 — 语法高亮
        case 'MARK_DOWN':
            // Markdown 报告 — 包含 ECharts 图表渲染
        case 'RESULT_SET':
            // 查询结果集 — 表格 + 推荐图表
    }
};
```

---

## 辅助体系详解

### Prompt 模板体系

**目录**: `data-agent-management/src/main/resources/prompts/`（17 个 `.txt` 文件）

Prompt 的加载和使用形成三层架构：

```
PromptLoader (底层)
  ├── 从 classpath 加载 prompts/*.txt 文件
  ├── ConcurrentHashMap 缓存（避免重复读取）
  └── loadPrompt("intent-recognition") → "prompts/intent-recognition.txt" 的内容

PromptConstant (注册层)
  ├── 17 个静态方法，每个方法创建一个 PromptTemplate
  ├── getIntentRecognitionPromptTemplate() → new PromptTemplate(PromptLoader.loadPrompt("intent-recognition"))
  └── getPlannerPromptTemplate() → new PromptTemplate(PromptLoader.loadPrompt("planner"))

PromptHelper (构建层)
  ├── 组装参数 Map，渲染模板
  ├── buildIntentRecognitionPrompt(multiTurn, query)
  │     params = {"multi_turn": ..., "latest_query": ..., "format": ...}
  │     return PromptConstant.getIntentRecognitionPromptTemplate().render(params)
  └── buildReportGeneratorPromptWithOptimization(...)
        params = {"user_requirements_and_plan": ..., "analysis_steps_and_data": ..., ...}
        return PromptConstant.getReportGeneratorPlainPromptTemplate().render(params)
```

#### 所有 Prompt 模板一览

| 模板文件 | 用途 | 关键参数 |
|---|---|---|
| `intent-recognition.txt` | 意图识别（闲聊 vs 数据分析） | `multi_turn`, `latest_query`, `format` |
| `evidence-query-rewrite.txt` | 查询改写（用于向量检索） | `multi_turn`, `latest_query`, `format` |
| `query-enhancement.txt` | 查询增强（澄清+扩展） | `current_time_info`, `evidence`, `multi_turn`, `latest_query`, `format` |
| `feasibility-assessment.txt` | 可行性评估 | `canonical_query`, `recalled_schema`, `evidence`, `multi_turn` |
| `mix-selector.txt` | Schema 表精选 | `schema_info`, `question`, `evidence` |
| `planner.txt` | 执行计划生成 | `schema`, `evidence`, `semantic_model`, `plan_validation_error`, `format`, `user_question` |
| `new-sql-generate.txt` | SQL 首次生成 | `dialect`, `schema_info`, `evidence`, `question`, `execution_description` |
| `sql-error-fixer.txt` | SQL 错误修复 | `dialect`, `error_message`, `schema_info`, `execution_description`, `error_sql`, `question`, `evidence` |
| `semantic-consistency.txt` | SQL 语义检查 | `dialect`, `execution_description`, `sql`, `schema_info`, `user_query`, `evidence` |
| `python-generator.txt` | Python 代码生成 | `python_memory`, `python_timeout`, `database_schema`, `sample_input`, `plan_description` |
| `python-analyze.txt` | Python 结果分析 | `user_query`, `python_output` |
| `data-view-analyze.txt` | 图表类型推荐 | `format` |
| `report-generator-plain.txt` | 报告生成 | `json_example`, `user_requirements_and_plan`, `analysis_steps_and_data`, `summary_and_recommendations`, `optimization_section` |
| `agent-knowledge.txt` | Agent 知识注入 | `agentKnowledge` |
| `business-knowledge.txt` | 业务术语注入 | `businessKnowledge` |
| `semantic-model.txt` | 语义模型注入 | `semanticModel` |
| `json-fix.txt` | JSON 格式修复 | `json_string`, `error_message` |

### 工具类体系

#### StateUtil — 类型安全的状态读取

**文件**: `util/StateUtil.java`

所有节点通过 StateUtil 读取全局状态。它封装了类型转换、默认值、反序列化等通用逻辑：

```java
// 读取字符串（缺失时抛异常）
String query = StateUtil.getStringValue(state, INPUT_KEY);

// 读取字符串（带默认值）
String context = StateUtil.getStringValue(state, MULTI_TURN_CONTEXT, "");

// 读取并反序列化为 DTO（自动处理 HashMap → DTO 的转换）
QueryEnhanceOutputDTO dto = StateUtil.getObjectValue(state, QUERY_ENHANCE_NODE_OUTPUT, QueryEnhanceOutputDTO.class);

// 读取列表
List<Map<String, String>> results = StateUtil.getListValue(state, SQL_RESULT_LIST_MEMORY);

// 判断是否有值
if (StateUtil.hasValue(state, EVIDENCE)) { ... }

// 快捷获取规范化查询
String canonicalQuery = StateUtil.getCanonicalQuery(state);
```

#### PlanProcessUtil — 执行计划解析

**文件**: `util/PlanProcessUtil.java`

解析 `PLANNER_NODE_OUTPUT` JSON 为 `Plan` 对象，并提供步骤导航：

```java
// 获取完整 Plan
Plan plan = PlanProcessUtil.getPlan(state);

// 获取当前步骤号（默认1）
int stepNum = PlanProcessUtil.getCurrentStepNumber(state);

// 获取当前步骤
ExecutionStep step = PlanProcessUtil.getCurrentExecutionStep(state);

// 获取当前步骤的指令
String instruction = PlanProcessUtil.getCurrentExecutionStepInstruction(state);

// 添加步骤结果
Map<String, String> results = PlanProcessUtil.addStepResult(existingResults, stepNum, jsonResult);
// → {"step_1": "...", "step_2": "..."}
```

### 多数据库连接层

**目录**: `connector/impls/`

采用**策略模式 + 工厂模式 + Spring 自动注册**，支持 7 种数据库：

```
Accessor (接口)
  └─ AbstractAccessor (抽象类)
       ├── MySQLDBAccessor       @Service("mysqlAccessor")
       ├── PostgreSQLDBAccessor  @Service("postgreSQLAccessor")
       ├── OracleDBAccessor      @Service("oracleAccessor")
       ├── SQLServerDBAccessor   @Service("sqlserverAccessor")
       ├── H2DBAccessor          @Service("h2Accessor")
       ├── HiveDBAccessor        @Service("hiveAccessor")
       └── DaMengDBAccessor      @Service("damengAccessor")
```

每种数据库需要提供三个 `@Service` Bean：
1. `XxxDBAccessor` — 数据库操作（继承 `AbstractAccessor`）
2. `XxxJdbcConnectionPool` — 连接池管理（继承 `AbstractDBConnectionPool`）
3. `XxxJdbcDdl` — DDL 方言（继承 `AbstractJdbcDdl`）

加上一个 `@Component` 的 `DatasourceTypeHandler`。

**新增数据库的步骤**（以 ClickHouse 为例）：
1. 在 `BizDataSourceTypeEnum` 和 `DatabaseDialectEnum` 中添加枚举值
2. 在 `connector/impls/clickhouse/` 下创建三个 `@Service` 类
3. 在 `service/datasource/handler/impl/` 下创建一个 `@Component`
4. 在 `pom.xml` 中添加 JDBC 驱动依赖
5. 不需要修改任何工厂代码——Spring DI 自动注册

### Python 容器池

**文件**: `service/code/impls/AbstractCodePoolExecutorService.java`

这是一个精心设计的容器池管理器，使用**模板方法模式**：

```
CodePoolExecutorService (接口)
  └─ AbstractCodePoolExecutorService (模板方法)
       ├── DockerCodePoolExecutorService    ← 生产推荐
       ├── LocalCodePoolExecutorService     ← 本地开发
       └─ AiSimulationCodeExecutorService   ← 无执行环境时，AI 模拟结果
```

#### 容器池设计

```
┌─────────────────────────────────────────────┐
│             AbstractCodePoolExecutorService  │
├─────────────────────────────────────────────┤
│                                             │
│  核心容器池（Core Pool）                      │
│  ├── 容器1（常驻，始终可用）                    │
│  ├── 容器2（常驻，始终可用）                    │
│  └── ...                                    │
│                                             │
│  临时容器池（Temporary Pool）                 │
│  ├── 容器A（按需创建，空闲超时自动销毁）         │
│  └── 容器B（按需创建，空闲超时自动销毁）         │
│                                             │
│  任务队列（ArrayBlockingQueue）               │
│  ├── 任务1（等待空闲容器）                     │
│  └── 任务2（等待空闲容器）                     │
│                                             │
└─────────────────────────────────────────────┘
```

- 所有容器忙时，新任务进入队列等待
- 核心容器不够时，创建临时容器（有 `keepThreadAliveTime` 分钟的空闲超时）
- 临时容器超时后，启动一个 `ScheduledFuture` 倒计时，超时则销毁
- 如果临时容器在超时前被复用，取消倒计时

#### 容错机制

```java
// 容器异常时：销毁容器 → 任务重新入队
if (containerFailed) {
    stopContainer(container);
    removeContainer(container);
    taskQueue.put(task);  // 重新入队等待其他容器
}
```

#### 资源限制

| 参数 | 值 | 来源 |
|---|---|---|
| 容器内存 | 500 MB | `CodeExecutorProperties.limitMemory` |
| 代码执行超时 | 60 秒 | `CodeExecutorProperties.codeTimeout` |
| 容器超时 | 3000 秒 | `CodeExecutorProperties.containerTimeout` |
| 最大重试 | 5 次 | `CodeExecutorProperties.pythonMaxTriesCount` |
| Docker 网络模式 | none（无网络） | 默认配置 |

---

## 关键设计模式总结

### 1. 状态机模式（StateGraph）

```
OverAllState = Map<String, Object>   // 全局共享状态
NodeAction.apply(state) → Map        // 状态变换函数
EdgeAction.apply(state) → String     // 转移条件
KeyStrategy.REPLACE                   // 后写覆盖前写
```

整个图在 `DataAgentConfiguration.nl2sqlGraph()` 中声明式定义，节点之间的拓扑关系一目了然。

### 2. 策略模式 + 工厂模式 + 自动注册（数据库连接层）

```
Accessor / Ddl / DBConnectionPool 接口  ← 策略
每种数据库一个 @Service 实现              ← 具体策略
AccessorFactory / DBConnectionPoolFactory ← 工厂（通过 Spring DI 自动注册）
```

新增数据库类型只需添加实现类，无需修改工厂代码。

### 3. 观察者模式（SSE 流式推送）

```
GraphServiceImpl（生产者）→ Sinks.Many（管道）→ Spring WebFlux（传输）→ EventSource（消费者）
```

图执行线程和 HTTP 响应线程完全解耦，通过 `Sinks.Many` 管道连接。

### 4. 模板方法模式（Python 容器池）

```
AbstractCodePoolExecutorService 定义容器池管理骨架
子类实现 createNewContainer / execTaskInContainer / stopContainer
```

核心容器 + 临时容器的池化设计，兼顾性能和资源效率。

### 5. 责任链模式（SQL 重试循环）

```
SqlGenerateNode → SemanticConsistencyNode → SqlExecuteNode
  ↑ 失败时通过 Dispatcher 路由回 SqlGenerateNode，带上错误信息
```

SqlRetryDto 是贯穿责任链的错误信息载体，区分"语义检查失败"和"执行失败"两种重试原因。

---

## 调试与学习建议

### 1. 启动项目

```bash
# 后端（H2 内存数据库，零配置）
cd data-agent-management
./mvnw spring-boot:run -Dspring-boot.run.profiles=h2

# 前端
cd data-agent-frontend
npm install && npm run dev

# 访问 http://localhost:3000
```

### 2. 断点调试

在以下位置打断点，然后发一个请求，跟着调试器走完全流程：

1. `IntentRecognitionNode.apply()` — 第一个节点
2. `PlannerNode.apply()` — 计划生成
3. `SqlGenerateNode.apply()` — SQL 生成
4. `SqlExecuteNode.apply()` — SQL 执行
5. `ReportGeneratorNode.apply()` — 报告生成
6. `GraphServiceImpl.handleStreamNodeOutput()` — SSE 推送

### 3. 阅读 Prompt

所有 Prompt 模板在 `resources/prompts/` 目录。理解了 Prompt 就理解了每个节点的行为：
- 先读 `planner.txt`（最关键，定义了整个执行策略）
- 再读 `new-sql-generate.txt`（SQL 生成的核心）
- 最后读 `python-generator.txt`（Python 代码生成的约束）

### 4. 阅读测试

`src/test/` 下有 110+ 测试类，每个节点的测试能帮你理解输入/输出：
- `IntentRecognitionNodeTest` — 意图识别测试
- `PlannerNodeTest` — 计划生成测试
- `SqlGenerateNodeTest` — SQL 生成测试
- `SqlExecuteNodeTest` — SQL 执行测试
- `ReportGeneratorNodeTest` — 报告生成测试

### 5. H2 控制台

开发模式下可以访问 `http://localhost:8065/h2-console` 查看内存数据库中的数据，包括 Agent 配置、数据源、知识库等。
