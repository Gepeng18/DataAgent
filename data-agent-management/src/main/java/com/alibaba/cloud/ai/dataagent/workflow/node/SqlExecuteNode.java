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

import static com.alibaba.cloud.ai.dataagent.constant.Constant.PLAN_CURRENT_STEP;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_EXECUTE_NODE_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_GENERATE_COUNT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_GENERATE_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_REGENERATE_REASON;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_RESULT_LIST_MEMORY;

import com.alibaba.cloud.ai.dataagent.bo.DbConfigBO;
import com.alibaba.cloud.ai.dataagent.bo.schema.DisplayStyleBO;
import com.alibaba.cloud.ai.dataagent.bo.schema.ResultBO;
import com.alibaba.cloud.ai.dataagent.bo.schema.ResultSetBO;
import com.alibaba.cloud.ai.dataagent.connector.DbQueryParameter;
import com.alibaba.cloud.ai.dataagent.connector.accessor.Accessor;
import com.alibaba.cloud.ai.dataagent.constant.Constant;
import com.alibaba.cloud.ai.dataagent.dto.datasource.SqlRetryDto;
import com.alibaba.cloud.ai.dataagent.dto.planner.ExecutionStep;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.prompt.PromptHelper;
import com.alibaba.cloud.ai.dataagent.properties.DataAgentProperties;
import com.alibaba.cloud.ai.dataagent.service.llm.LlmService;
import com.alibaba.cloud.ai.dataagent.service.nl2sql.Nl2SqlService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.DatabaseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.JsonParseUtil;
import com.alibaba.cloud.ai.dataagent.util.JsonUtil;
import com.alibaba.cloud.ai.dataagent.util.MarkdownParserUtil;
import com.alibaba.cloud.ai.dataagent.util.PlanProcessUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * SQL 执行节点（SqlExecute）—— 工作流中唯一直接操作数据库的节点。
 *
 * <p>在工作流中的位置（SQL 分支内部）：
 * <pre>
 *   SqlGenerateNode → SemanticConsistencyNode → <b>SqlExecuteNode（本节点）</b>
 * </pre>
 *
 * <p>本节点负责两件事：
 * <ol>
 *   <li><b>执行 SQL 并获取结果</b>：通过 {@link Accessor} 将 SQL 发送到目标数据库执行，
 *       获取结构化的 {@link ResultSetBO} 结果集</li>
 *   <li><b>智能图表推荐</b>（可选）：将 SQL 结果样本发送给 LLM，让 LLM 推荐最佳的数据可视化方式
 *       （折线图、柱状图、饼图等），返回 {@link DisplayStyleBO} 图表配置</li>
 * </ol>
 *
 * <p>执行结果会通过 SSE（Server-Sent Events）流式推送给前端，实现"边执行边展示"的用户体验。
 * 同时，结果数据会写入状态，供下游的 Python 分析节点和报告生成节点消费。
 *
 * <p>涉及的 AI 概念：
 * <ul>
 *   <li><b>Tool Use / Function Calling</b>：本节点可以看作 LLM 调用"数据库查询"工具的执行器</li>
 *   <li><b>ReAct 模式</b>：SQL 执行失败时会设置 SQL_REGENERATE_REASON，触发 SqlGenerateNode 重新生成 SQL，
 *       形成"生成→校验→执行→失败→重新生成"的自纠正循环</li>
 *   <li><b>LLM 辅助可视化</b>：利用 LLM 分析查询结果，自动推荐合适的图表类型和配置</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see ResultSetBO SQL 执行结果的封装结构
 * @see DisplayStyleBO LLM 推荐的图表可视化配置
 * @see Accessor 数据库访问抽象层，支持多种数据库类型
 */
@Slf4j
@Component
@AllArgsConstructor
public class SqlExecuteNode implements NodeAction {

	private final DatabaseUtil databaseUtil;

	private final Nl2SqlService nl2SqlService;

	private final LlmService llmService;

	private final DataAgentProperties properties;

	private final JsonParseUtil jsonParseUtil;

	/**
	 * 图表推荐时发送给 LLM 的样本数据行数限制。
	 * 避免将大量查询结果全部发送给 LLM，导致 Prompt 过长超出 token 限制。
	 */
	private static final int SAMPLE_DATA_NUMBER = 20;

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到此节点时自动调用。
	 *
	 * <p><b>输入状态读取：</b>
	 * <ul>
	 *   <li>{@code PLAN_CURRENT_STEP} — 当前执行步骤编号</li>
	 *   <li>{@code SQL_GENERATE_OUTPUT} — SqlGenerateNode 生成的 SQL 语句</li>
	 *   <li>{@code AGENT_ID} — 当前 Agent 的 ID，用于获取对应的数据源配置</li>
	 * </ul>
	 *
	 * <p><b>核心逻辑：</b>
	 * <ol>
	 *   <li>从状态中取出 SQL 并清理格式（去除 Markdown 代码块标记等）</li>
	 *   <li>根据 Agent ID 获取对应的数据源配置（{@link DbConfigBO}）</li>
	 *   <li>委托 {@link #executeSqlQuery} 方法执行 SQL 并返回流式结果</li>
	 * </ol>
	 *
	 * <p><b>输出状态写入：</b>（由 executeSqlQuery 内部写入）
	 * <ul>
	 *   <li>{@code SQL_EXECUTE_NODE_OUTPUT} — 所有步骤的 SQL 执行结果（按步骤编号索引）</li>
	 *   <li>{@code SQL_REGENERATE_REASON} — 执行失败时的错误信息</li>
	 *   <li>{@code SQL_RESULT_LIST_MEMORY} — 查询结果数据列表（供 Python 分析节点使用）</li>
	 *   <li>{@code PLAN_CURRENT_STEP} — 推进到下一步</li>
	 *   <li>{@code SQL_GENERATE_COUNT} — 重置 SQL 生成重试计数</li>
	 * </ul>
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		Integer currentStep = PlanProcessUtil.getCurrentStepNumber(state);

		// 从状态中取出 SqlGenerateNode 生成的 SQL 语句
		String sqlQuery = StateUtil.getStringValue(state, SQL_GENERATE_OUTPUT);
		// 清理 SQL 格式：去除 LLM 可能生成的 Markdown 代码块标记（```sql ... ```）和多余空白
		sqlQuery = nl2SqlService.sqlTrim(sqlQuery);

		log.info("Executing SQL query: {}", sqlQuery);

		// 获取当前 Agent 的 ID，每个 Agent 绑定不同的数据源
		String agentIdStr = StateUtil.getStringValue(state, Constant.AGENT_ID);
		if (StringUtils.isBlank(agentIdStr)) {
			throw new IllegalStateException("Agent ID cannot be empty.");
		}

		Long agentId = Long.valueOf(agentIdStr);

		// 根据 Agent ID 动态获取数据源配置（包括数据库连接信息、Schema 等）
		DbConfigBO dbConfig = databaseUtil.getAgentDbConfig(agentId);

		return executeSqlQuery(state, currentStep, sqlQuery, dbConfig, agentId);
	}

	/**
	 * 执行 SQL 查询并处理结果的核心方法。
	 *
	 * <p>采用"业务逻辑优先"模式：
	 * <ol>
	 *   <li>在 Flux.create 内部同步执行 SQL 查询（实际数据库操作）</li>
	 *   <li>通过 Flux emitter 将执行过程（开始、SQL 展示、结果、完成/失败）逐步推送为 SSE 消息</li>
	 *   <li>执行成功后调用 LLM 获取图表配置，丰富结果展示</li>
	 *   <li>将最终结果写入状态 Map，供 FluxUtil 回调提取</li>
	 * </ol>
	 *
	 * <p>这种设计将"数据库执行"和"SSE 流式展示"统一到一个 Flux 管道中，
	 * 既保证了数据库操作的同步性，又实现了前端流式体验。
	 *
	 * @param StateGraph 的全局状态对象
	 * @param currentStep 当前执行步骤编号
	 * @param sqlQuery 待执行的 SQL 语句
	 * @param dbConfig 数据源配置
	 * @param agentId Agent ID
	 * @return 包含流式生成器的 Map
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> executeSqlQuery(OverAllState state, Integer currentStep, String sqlQuery,
			DbConfigBO dbConfig, Long agentId) {
		// 构建数据库查询参数
		DbQueryParameter dbQueryParameter = new DbQueryParameter();
		dbQueryParameter.setSql(sqlQuery);
		dbQueryParameter.setSchema(dbConfig.getSchema());

		// 获取数据库访问器（Accessor），它会根据数据库类型路由到对应的连接池（MySQL/H2/PostgreSQL 等）
		Accessor dbAccessor = databaseUtil.getAgentAccessor(agentId);
		// 用于存储最终业务逻辑结果的容器，在 Flux.create 内部填充，在 FluxUtil 回调中提取
		final Map<String, Object> result = new HashMap<>();

		// 构建 SSE 流式响应：先推送进度消息（开始执行、SQL 展示），再执行实际数据库查询
		Flux<ChatResponse> displayFlux = Flux.create(emitter -> {
			// 推送执行进度消息，前端实时展示
			emitter.next(ChatResponseUtil.createResponse("开始执行SQL..."));
			emitter.next(ChatResponseUtil.createResponse("执行SQL查询："));
			// 用 SQL 类型标记符包裹 SQL 语句，前端据此高亮展示
			emitter.next(ChatResponseUtil.createPureResponse(TextType.SQL.getStartSign()));
			emitter.next(ChatResponseUtil.createResponse(sqlQuery));
			emitter.next(ChatResponseUtil.createPureResponse(TextType.SQL.getEndSign()));
			ResultBO resultBO = ResultBO.builder().build();

			try {
				// ============ 核心数据库操作 ============
				// 通过 Accessor 执行 SQL 并获取结构化结果集（ResultSetBO 包含列名、列类型、数据行等）
				ResultSetBO resultSetBO = dbAccessor.executeSqlAndReturnObject(dbConfig, dbQueryParameter);
				// 调用 LLM 根据查询结果推荐图表可视化配置（折线图/柱状图/饼图等）
				DisplayStyleBO displayStyleBO = enrichResultSetWithChartConfig(state, resultSetBO);
				resultBO.setResultSet(resultSetBO);
				resultBO.setDisplayStyle(displayStyleBO);

				// 序列化结果为 JSON，供前端展示和下游节点消费
				String strResultSetJson = JsonUtil.getObjectMapper().writeValueAsString(resultSetBO);
				String strResultJson = JsonUtil.getObjectMapper().writeValueAsString(resultBO);

				// ============ 推送成功消息 ============
				emitter.next(ChatResponseUtil.createResponse("执行SQL完成"));
				emitter.next(ChatResponseUtil.createResponse("SQL查询结果："));
				// 用 RESULT_SET 类型标记符包裹结果 JSON，前端据此渲染数据表格
				emitter.next(ChatResponseUtil.createPureResponse(TextType.RESULT_SET.getStartSign()));
				emitter.next(ChatResponseUtil.createPureResponse(strResultJson));
				emitter.next(ChatResponseUtil.createPureResponse(TextType.RESULT_SET.getEndSign()));

				// ============ 累积步骤结果 ============
				// 将当前步骤的结果追加到已有结果 Map 中（key=步骤编号，value=结果 JSON）
				// 多步骤执行时，每个步骤的结果都会被保留，供报告生成节点汇总
				Map<String, String> existingResults = StateUtil.getObjectValue(state, SQL_EXECUTE_NODE_OUTPUT,
						Map.class, new HashMap<>());
				Map<String, String> updatedResults = PlanProcessUtil.addStepResult(existingResults, currentStep,
						strResultSetJson);

				log.info("SQL execution successful, result count: {}",
						resultSetBO.getData() != null ? resultSetBO.getData().size() : 0);

				// 回写最终执行的 SQL 到当前步骤的 ToolParameters 中，报告节点需要使用实际执行的 SQL
				ExecutionStep.ToolParameters currentStepParams = PlanProcessUtil.getCurrentExecutionStep(state)
					.getToolParameters();
				currentStepParams.setSqlQuery(sqlQuery);

				// ============ 写入最终业务逻辑结果 ============
				// - SQL_EXECUTE_NODE_OUTPUT: 累积的所有步骤执行结果
				// - SQL_REGENERATE_REASON: 清空重试原因（执行成功，不需要重新生成）
				// - SQL_RESULT_LIST_MEMORY: 原始数据列表（供 Python 分析节点直接使用）
				// - PLAN_CURRENT_STEP: 推进到下一步（+1）
				// - SQL_GENERATE_COUNT: 重置 SQL 生成重试计数（用于控制重试次数上限）
				result.putAll(Map.of(SQL_EXECUTE_NODE_OUTPUT, updatedResults, SQL_REGENERATE_REASON,
						SqlRetryDto.empty(), SQL_RESULT_LIST_MEMORY, resultSetBO.getData(), PLAN_CURRENT_STEP,
						currentStep + 1, SQL_GENERATE_COUNT, 0));
			}
			catch (Exception e) {
				// SQL 执行失败：记录错误原因，触发 SqlGenerateNode 重新生成 SQL（ReAct 自纠正循环）
				String errorMessage = e.getMessage();
				log.error("SQL execution failed - SQL as follows: \n {} \n ", sqlQuery, e);
				result.put(SQL_REGENERATE_REASON, SqlRetryDto.sqlExecute(errorMessage));
				emitter.next(ChatResponseUtil.createResponse("SQL执行失败: " + errorMessage));
			}
			finally {
				emitter.complete();
			}
		});

		// 将 displayFlux 包装为 GraphResponse<StreamingOutput> 流，
		// FluxUtil 的回调函数直接返回 result Map（在 Flux.create 内部已填充完成）
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, v -> result, displayFlux);
		return Map.of(SQL_EXECUTE_NODE_OUTPUT, generator);
	}

	/**
	 * 调用 LLM 获取图表配置信息，实现"智能数据可视化推荐"。
	 *
	 * <p>工作原理：
	 * <ol>
	 *   <li>取查询结果的前 {@value #SAMPLE_DATA_NUMBER} 行作为样本数据（避免 Prompt 过长）</li>
	 *   <li>将样本数据连同用户问题一起发送给 LLM</li>
	 *   <li>LLM 分析数据特征和用户意图，返回推荐的图表配置（图表类型、X/Y 轴字段、标题等）</li>
	 *   <li>解析 LLM 返回的 JSON 为 {@link DisplayStyleBO} 对象</li>
	 * </ol>
	 *
	 * <p>此功能可通过 {@code data-agent.enable-sql-result-chart} 配置项开关。
	 * 如果 LLM 调用超时或失败，不会中断主流程（图表配置返回 null，前端以默认表格形式展示）。
	 *
	 * @param state 全局状态，用于获取用户查询和当前上下文
	 * @param resultSetBO SQL 执行结果集
	 * @return LLM 推荐的图表配置，失败时返回 null（前端使用默认表格展示）
	 */
	private DisplayStyleBO enrichResultSetWithChartConfig(OverAllState state, ResultSetBO resultSetBO) {
		DisplayStyleBO displayStyle = new DisplayStyleBO();
		// 检查图表推荐功能是否开启，未开启则默认以表格展示
		if (!this.properties.isEnableSqlResultChart()) {
			log.debug("Sql result chart is disabled, set display style as table default");
			displayStyle.setType("table");
			return displayStyle;
		}

		try {
			// 获取用户查询，帮助 LLM 理解数据可视化的上下文
			String userQuery = StateUtil.getCanonicalQuery(state);

			// 只取前 SAMPLE_DATA_NUMBER 行数据作为样本，避免将全部查询结果发送给 LLM
			// 大量数据会导致 Prompt 超出 LLM 的上下文窗口限制（token limit）
			String sqlResultJson = JsonUtil.getObjectMapper()
				.writeValueAsString(resultSetBO.getData() != null
						? resultSetBO.getData().stream().limit(SAMPLE_DATA_NUMBER).toList() : null);

			// 构建用户提示词：包含用户问题和样本数据，引导 LLM 推荐合适的图表类型
			String userPrompt = String.format("""
					# 正式任务

					<最新>用户输入: %s
					范例数据: %s

					# 输出
					""", userQuery != null ? userQuery : "数据可视化", sqlResultJson);

			// 加载图表分析的系统提示词模板（定义了 LLM 应返回的图表配置 JSON 格式）
			String fullPrompt = PromptHelper.buildDataViewAnalysisPrompt();
			// 将模板按分隔符拆分为系统提示词和用户提示词两部分
			// 这是 Spring AI 的常见模式：systemPrompt 定义角色和输出格式约束，userPrompt 包含具体任务
			String[] parts = fullPrompt.split("=== 用户输入 ===", 2);
			String systemPrompt = parts[0].trim();

			log.debug("Built chart config generation system prompt as follows \n {} \n", systemPrompt);
			log.debug("Built chart config generation user prompt as follows \n {} \n", userPrompt);

			// 调用 LLM 生成图表配置：
			// 1. llmService.call() 发送 system + user 双角色 Prompt
			// 2. toStringFlux() 将流式 ChatResponse 转为字符串 Flux
			// 3. collect() 收集所有 token 拼接为完整 JSON 字符串
			// 4. block() 阻塞等待结果，设置超时时间防止 LLM 响应过慢阻塞整个工作流
			String chartConfigJson = llmService.toStringFlux(llmService.call(systemPrompt, userPrompt))
				.collect(StringBuilder::new, StringBuilder::append)
				.map(StringBuilder::toString)
				.block(Duration.ofMillis(properties.getEnrichSqlResultTimeout()));
			if (chartConfigJson != null && !chartConfigJson.trim().isEmpty()) {
				// LLM 可能返回 Markdown 代码块包裹的 JSON（如 ```json ... ```），需要提取纯文本
				String content = MarkdownParserUtil.extractText(chartConfigJson.trim());
				// 尝试将 LLM 返回的文本解析为 DisplayStyleBO 对象
				// tryConvertToObject 内部做了容错处理，解析失败返回 null
				displayStyle = jsonParseUtil.tryConvertToObject(content, DisplayStyleBO.class);
				log.debug("Successfully enriched ResultSetBO with chart config: type={}, title={}, x={}, y={}",
						displayStyle.getType(), displayStyle.getTitle(), displayStyle.getX(), displayStyle.getY());
				return displayStyle;
			}
			else {
				log.warn("LLM returned empty chart config, using default settings");
			}
		}
		catch (Exception e) {
			log.error("Failed to enrich ResultSetBO with chart config", e);
			// 图表推荐失败不影响主流程，返回 null 后前端使用默认表格展示
		}
		return null;
	}

}
