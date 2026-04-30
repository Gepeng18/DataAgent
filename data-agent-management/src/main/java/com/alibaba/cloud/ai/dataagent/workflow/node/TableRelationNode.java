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

import static com.alibaba.cloud.ai.dataagent.constant.Constant.AGENT_ID;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.DB_DIALECT_TYPE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.EVIDENCE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.GENEGRATED_SEMANTIC_MODEL_PROMPT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SQL_GENERATE_SCHEMA_MISSING_ADVICE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_RELATION_EXCEPTION_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_RELATION_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_RELATION_RETRY_COUNT;
import static com.alibaba.cloud.ai.dataagent.prompt.PromptHelper.buildSemanticModelPrompt;

import com.alibaba.cloud.ai.dataagent.bo.DbConfigBO;
import com.alibaba.cloud.ai.dataagent.dto.schema.SchemaDTO;
import com.alibaba.cloud.ai.dataagent.dto.schema.TableDTO;
import com.alibaba.cloud.ai.dataagent.entity.AgentDatasource;
import com.alibaba.cloud.ai.dataagent.entity.LogicalRelation;
import com.alibaba.cloud.ai.dataagent.entity.SemanticModel;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.service.datasource.AgentDatasourceService;
import com.alibaba.cloud.ai.dataagent.service.datasource.DatasourceService;
import com.alibaba.cloud.ai.dataagent.service.nl2sql.Nl2SqlService;
import com.alibaba.cloud.ai.dataagent.service.schema.SchemaService;
import com.alibaba.cloud.ai.dataagent.service.semantic.SemanticModelService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.DatabaseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * 表关系推断节点 —— StateGraph 工作流中负责推断表间关系、精选 Schema 列、加载语义模型的核心节点。
 *
 * <h3>在工作流中的位置</h3>
 * <p>... → SchemaRecallNode → <b>TableRelationNode（当前节点）</b> → FeasibilityAssessment → Planner → ...</p>
 *
 * <h3>核心职责</h3>
 * <p>SchemaRecallNode 通过向量检索召回了与用户问题相关的表和列文档，但这些是"粗筛"结果——
 *    可能包含多余的列、缺少表间 JOIN 关系信息、也没有业务语义映射。
 *    本节点的作用是对粗筛结果做"精加工"，具体包括：</p>
 * <ol>
 *   <li>将向量召回的表和列文档组装为完整的 SchemaDTO（包含数据库名、表结构、外键信息）</li>
 *   <li>合并逻辑外键（用户手动配置的表间关联关系）</li>
 *   <li>调用 LLM 对 Schema 进行精选（fineSelect）：LLM 根据用户问题和证据，从候选列中筛选出真正需要的列</li>
 *   <li>加载与最终选中表相关的语义模型（Semantic Model），将业务术语映射到数据库字段</li>
 * </ol>
 *
 * <h3>涉及的 AI 概念</h3>
 * <ul>
 *   <li><b>Schema Selection / Schema Pruning（模式精选）</b>：
 *       NL2SQL 中减少无关列噪音的关键技术。向量检索可能召回数十个表的数百列，
 *       如果全部交给 SQL 生成节点，LLM 容易混淆或生成错误 JOIN。
 *       本节点利用 LLM 的理解能力，只保留与当前查询真正相关的列。</li>
 *   <li><b>Semantic Model（语义模型）</b>：
 *       将业务术语映射到数据库字段的配置。例如 "GMV" 映射到 order_table.total_amount，
 *       "活跃用户" 映射到 user_table.status='active' AND last_login_date > ?。
 *       语义模型作为 Prompt 的一部分提供给 SQL 生成节点，帮助 LLM 理解业务含义。</li>
 *   <li><b>Logical Foreign Key（逻辑外键）</b>：
 *       数据库中未必有物理外键约束（尤其在大数据平台/数仓中），但表之间实际存在关联关系。
 *       用户可以在管理界面手动配置这些"逻辑外键"（如 order_table.user_id = user_table.id），
 *       本节点会在 Schema 中补充这些信息，帮助 LLM 生成正确的 JOIN 语句。</li>
 *   <li><b>Schema Advice（Schema 补充建议）</b>：
 *       在 SQL 语义检查（SemanticConsistencyNode）中如果发现 Schema 信息不足，
 *       会生成补充建议（如"需要了解 user_table 的字段"），并通过全局状态传递到本节点。
 *       本节点在下一次执行时会根据建议补充召回相关表和列（即 SQL 重试流程）。</li>
 * </ul>
 *
 * <h3>三阶段处理流程</h3>
 * <ol>
 *   <li><b>Stage 1 - 组装初始 Schema</b>：将向量召回的表/列文档 + 逻辑外键组装为 SchemaDTO</li>
 *   <li><b>Stage 2 - LLM 精选</b>：调用 Nl2SqlService.fineSelect，让 LLM 从候选列中筛选出查询需要的列</li>
 *   <li><b>Stage 3 - 加载语义模型</b>：根据 LLM 精选后保留的表名，加载对应的语义模型映射</li>
 * </ol>
 *
 * @see SchemaService Schema 构建服务（文档 → SchemaDTO 转换）
 * @see com.alibaba.cloud.ai.dataagent.service.nl2sql.Nl2SqlService NL2SQL 服务（LLM Schema 精选）
 * @see com.alibaba.cloud.ai.dataagent.service.semantic.SemanticModelService 语义模型服务
 * @see com.alibaba.cloud.ai.dataagent.dto.schema.SchemaDTO Schema 数据传输对象
 * @author zhangshenghang
 */
@Slf4j
@Component
@AllArgsConstructor
public class TableRelationNode implements NodeAction {

	private final SchemaService schemaService;

	private final Nl2SqlService nl2SqlService;

	private final SemanticModelService semanticModelService;

	private final DatabaseUtil databaseUtil;

	private final DatasourceService datasourceService;

	private final AgentDatasourceService agentDatasourceService;

	/**
	 * 节点执行入口，由 StateGraph 框架在到达该节点时自动调用。
	 *
	 * <h3>输入状态读取</h3>
	 * <ul>
	 *   <li>{@code CANONICAL_QUERY} —— 经 QueryEnhance 改写后的标准查询语句（StateUtil.getCanonicalQuery 提取）</li>
	 *   <li>{@code EVIDENCE} —— EvidenceRecallNode 检索到的证据文本</li>
	 *   <li>{@code TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT} —— SchemaRecallNode 召回的表文档列表</li>
	 *   <li>{@code COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT} —— SchemaRecallNode 召回的列文档列表</li>
	 *   <li>{@code AGENT_ID} —— 当前智能体 ID</li>
	 *   <li>{@code SQL_GENERATE_SCHEMA_MISSING_ADVICE}（可选）—— SQL 语义检查节点的 Schema 补充建议，
	 *       非空时表示本节点处于 SQL 重试流程中</li>
	 * </ul>
	 *
	 * <h3>LLM 调用逻辑</h3>
	 * <p>通过 {@link com.alibaba.cloud.ai.dataagent.service.nl2sql.Nl2SqlService#fineSelect} 调用 LLM，
	 *    将完整的 SchemaDTO + 用户问题 + 证据 + 可选的补充建议作为 Prompt，
	 *    让 LLM 从候选列中精选出查询真正需要的列，并推断表间 JOIN 关系。
	 *    LLM 以流式（Flux）返回结果。</p>
	 *
	 * <h3>输出状态写入</h3>
	 * <ul>
	 *   <li>{@code TABLE_RELATION_OUTPUT} —— LLM 精选后的 SchemaDTO 流式生成器，
	 *       下游 SQL 生成节点从中获取最终的表结构和列信息</li>
	 *   <li>{@code DB_DIALECT_TYPE} —— 数据库方言类型（如 MySQL、PostgreSQL、H2 等），
	 *       下游 SQL 生成节点据此生成对应方言的 SQL</li>
	 *   <li>{@code GENEGRATED_SEMANTIC_MODEL_PROMPT} —— 语义模型映射文本，
	 *       将拼接到 SQL 生成节点的 Prompt 中</li>
	 *   <li>{@code TABLE_RELATION_RETRY_COUNT} —— 重试计数（初始为 0）</li>
	 *   <li>{@code TABLE_RELATION_EXCEPTION_OUTPUT} —— 异常信息（初始为空）</li>
	 * </ul>
	 *
	 * @param state StateGraph 的全局共享状态对象
	 * @return 包含流式生成器和状态值的 Map
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// 从全局状态中读取 SchemaRecallNode 的输出：用户问题（改写后）、证据、表/列文档、Agent ID
		String canonicalQuery = StateUtil.getCanonicalQuery(state);

		String evidence = StateUtil.getStringValue(state, EVIDENCE);
		List<Document> tableDocuments = StateUtil.getDocumentList(state, TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT);
		List<Document> columnDocuments = StateUtil.getDocumentList(state, COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT);
		String agentIdStr = StateUtil.getStringValue(state, AGENT_ID);

		// 获取当前 Agent 的数据库连接配置（包含 JDBC URL、方言类型等）
		DbConfigBO agentDbConfig = databaseUtil.getAgentDbConfig(Long.valueOf(agentIdStr));

		// Stage 0：获取逻辑外键信息。
		// 逻辑外键是用户在管理界面手动配置的表间关联关系（如 order.user_id = user.id），
		// 在数仓等没有物理外键的环境中尤为重要，帮助 LLM 生成正确的 JOIN 语句。
		// 这里只保留与当前召回表相关的外键，避免无关信息干扰 LLM。
		List<String> logicalForeignKeys = getLogicalForeignKeys(Long.valueOf(agentIdStr), tableDocuments);
		log.info("Found {} logical foreign keys for agent: {}", logicalForeignKeys.size(), agentIdStr);

		// Stage 1：组装初始 SchemaDTO。
		// 将向量召回的表/列文档转换为结构化的 SchemaDTO 对象，
		// 并将逻辑外键信息合并到 SchemaDTO 的 foreignKeys 字段中。
		SchemaDTO initialSchema = buildInitialSchema(agentIdStr, columnDocuments, tableDocuments, agentDbConfig,
				logicalForeignKeys);

		// resultMap 用于收集流式处理过程中产生的最终结果，
		// 它会在 LLM 精选完成的回调（Consumer<SchemaDTO>）中被填充。
		Map<String, Object> resultMap = new HashMap<>();
		// 将 DB_DIALECT_TYPE 添加到 resultMap，确保 LLM 精选完成后它被写入 state
		resultMap.put(DB_DIALECT_TYPE, agentDbConfig.getDialectType());
		// 初始化重试计数和异常信息，供 SQL 重试流程使用
		resultMap.put(TABLE_RELATION_RETRY_COUNT, 0);
		resultMap.put(TABLE_RELATION_EXCEPTION_OUTPUT, "");

		// Stage 2：LLM 精选 Schema。
		// processSchemaSelection 内部调用 nl2SqlService.fineSelect，将 SchemaDTO + 用户问题 + 证据
		// 组装为 Prompt 交给 LLM，LLM 从候选列中筛选出查询需要的列，并推断 JOIN 关系。
		// 返回的 schemaFlux 是一个 Flux<ChatResponse>，包含 LLM 流式输出的精选结果。
		// Consumer<SchemaDTO> 回调在 LLM 输出完成后触发，用于执行 Stage 3。
		Flux<ChatResponse> schemaFlux = processSchemaSelection(initialSchema, canonicalQuery, evidence, state,
				agentDbConfig, result -> {
					log.info("[{}] Schema processing result: {}", this.getClass().getSimpleName(), result);
					// 将 LLM 精选后的 SchemaDTO 存入 resultMap
					resultMap.put(TABLE_RELATION_OUTPUT, result);

					// Stage 3：加载语义模型映射。
					// 从 LLM 精选后的 SchemaDTO 中提取最终保留的表名，
					// 然后根据 agentId + 表名列表查询语义模型（业务术语 → 数据库字段的映射）。
					List<String> tableNames = result.getTable().stream().map(TableDTO::getName).toList();

					// 语义模型示例：{"GMV": "order_table.total_amount", "活跃用户": "user_table.status='active'"}
					// 这些映射会拼接到 SQL 生成节点的 Prompt 中，帮助 LLM 理解业务术语
					List<SemanticModel> semanticModels = semanticModelService
						.getByAgentIdAndTableNames(Long.valueOf(agentIdStr), tableNames);

					// 将语义模型格式化为 Prompt 文本，存入 resultMap
					String semanticModelPrompt = buildSemanticModelPrompt(semanticModels);
					resultMap.put(GENEGRATED_SEMANTIC_MODEL_PROMPT, semanticModelPrompt);
				});

		// 构建展示用 Flux 流，串联三段消息：
		// 1. preFlux：Stage 1 完成提示（"初始Schema构建完成"）
		// 2. schemaFlux：Stage 2 LLM 精选过程的流式输出（前端可实时看到 LLM 正在选择哪些列）
		// 3. postFlux：Stage 2/3 完成提示
		Flux<ChatResponse> preFlux = Flux.create(emitter -> {
			emitter.next(ChatResponseUtil.createResponse("开始构建初始Schema..."));
			emitter.next(ChatResponseUtil.createResponse("初始Schema构建完成."));
			emitter.complete();
		});
		Flux<ChatResponse> displayFlux = preFlux.concatWith(schemaFlux).concatWith(Flux.create(emitter -> {
			emitter.next(ChatResponseUtil.createResponse("开始处理Schema选择..."));
			emitter.next(ChatResponseUtil.createResponse("Schema选择处理完成."));
			emitter.complete();
		}));

		// 创建流式生成器：displayFlux 负责前端展示，v -> resultMap 负责在流消费完毕后写入全局状态。
		// 注意这里 resultMapper 使用的是 v -> resultMap（固定引用），因为 resultMap 是在回调中动态填充的。
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, v -> resultMap, displayFlux);

		// 返回值说明：
		// - TABLE_RELATION_OUTPUT：流式生成器，框架消费完流后将 resultMap 中的最终结果写入 state
		// - DB_DIALECT_TYPE：数据库方言类型，必须直接返回以确保下游节点立即可用（不等流消费完毕）
		// - TABLE_RELATION_RETRY_COUNT / TABLE_RELATION_EXCEPTION_OUTPUT：重试流程的初始状态
		return Map.of(TABLE_RELATION_OUTPUT, generator, DB_DIALECT_TYPE, agentDbConfig.getDialectType(),
				TABLE_RELATION_RETRY_COUNT, 0, TABLE_RELATION_EXCEPTION_OUTPUT, "");
	}

	/**
	 * Stage 1：将向量召回的文档和逻辑外键组装为完整的 SchemaDTO。
	 *
	 * <p>SchemaDTO 是后续 LLM 精选的输入，包含：</p>
	 * <ul>
	 *   <li>databaseName：数据库名称（从 JDBC URL 提取）</li>
	 *   <li>table：表结构列表（每个表包含列名、类型、注释等）</li>
	 *   <li>foreignKeys：外键关系列表（格式：table1.col1=table2.col2）</li>
	 * </ul>
	 *
	 * @param agentId             智能体 ID
	 * @param columnDocuments     向量召回的列文档列表
	 * @param tableDocuments      向量召回的表文档列表
	 * @param agentDbConfig       数据库连接配置
	 * @param logicalForeignKeys  逻辑外键列表
	 * @return 包含完整 Schema 信息的 SchemaDTO
	 */
	private SchemaDTO buildInitialSchema(String agentId, List<Document> columnDocuments, List<Document> tableDocuments,
			DbConfigBO agentDbConfig, List<String> logicalForeignKeys) {
		SchemaDTO schemaDTO = new SchemaDTO();

		// 从 JDBC URL 中提取数据库名称，用于 Schema 上下文
		schemaService.extractDatabaseName(schemaDTO, agentDbConfig);
		// 将向量召回的表/列 Document 转换为 SchemaDTO 中的结构化表和列信息
		schemaService.buildSchemaFromDocuments(agentId, columnDocuments, tableDocuments, schemaDTO);

		// 将逻辑外键信息合并到 schemaDTO 的 foreignKeys 字段
		if (logicalForeignKeys != null && !logicalForeignKeys.isEmpty()) {
			List<String> existingForeignKeys = schemaDTO.getForeignKeys();
			if (existingForeignKeys == null || existingForeignKeys.isEmpty()) {
				// 如果没有现有外键，直接设置
				schemaDTO.setForeignKeys(logicalForeignKeys);
			}
			else {
				// 合并现有外键和逻辑外键
				List<String> allForeignKeys = new ArrayList<>(existingForeignKeys);
				allForeignKeys.addAll(logicalForeignKeys);
				schemaDTO.setForeignKeys(allForeignKeys);
			}
			log.info("Merged {} logical foreign keys into schema for agent: {}", logicalForeignKeys.size(), agentId);
		}

		return schemaDTO;
	}

	/**
	 * Stage 2：调用 LLM 对 Schema 进行精选（fineSelect）。
	 *
	 * <p>将完整的 SchemaDTO（包含所有候选列）连同用户问题和证据一起交给 LLM，
	 *    LLM 从中筛选出与当前查询真正相关的列，并推断表间 JOIN 关系。
	 *    精选完成后通过 dtoConsumer 回调将结果传递给 Stage 3。</p>
	 *
	 * <p>如果全局状态中存在 SQL_GENERATE_SCHEMA_MISSING_ADVICE（SQL 语义检查节点的补充建议），
	 *    说明当前处于 SQL 重试流程，LLM 会根据建议额外召回缺失的表/列信息。</p>
	 *
	 * @param schemaDTO     初始 SchemaDTO（包含所有候选表和列）
	 * @param input         改写后的用户查询
	 * @param evidence      证据文本（来自 EvidenceRecallNode）
	 * @param state         全局状态（用于读取 Schema 补充建议）
	 * @param agentDbConfig 数据库配置
	 * @param dtoConsumer   LLM 精选完成后的回调，接收精炼后的 SchemaDTO
	 * @return 包含 LLM 流式精选结果的 Flux
	 */
	private Flux<ChatResponse> processSchemaSelection(SchemaDTO schemaDTO, String input, String evidence,
			OverAllState state, DbConfigBO agentDbConfig, Consumer<SchemaDTO> dtoConsumer) {
		// 检查是否有来自 SQL 语义检查节点的 Schema 补充建议
		// 非空时表示当前处于 SQL 重试流程：上一次生成的 SQL 因为 Schema 信息不足而失败，
		// 语义检查节点生成了补充建议（如"需要了解 order_table 的 status 字段含义"）
		String schemaAdvice = StateUtil.getStringValue(state, SQL_GENERATE_SCHEMA_MISSING_ADVICE, null);

		Flux<ChatResponse> schemaFlux;
		if (schemaAdvice != null) {
			log.info("[{}] Processing with schema supplement advice: {}", this.getClass().getSimpleName(),
					schemaAdvice);
			// 带补充建议的精选：LLM 在原有 Schema 基础上，额外关注建议中提到的表/列
			schemaFlux = nl2SqlService.fineSelect(schemaDTO, input, evidence, schemaAdvice, agentDbConfig, dtoConsumer);
		}
		else {
			log.info("[{}] Executing regular schema selection", this.getClass().getSimpleName());
			// 常规精选：LLM 根据用户问题和证据从候选列中筛选
			schemaFlux = nl2SqlService.fineSelect(schemaDTO, input, evidence, null, agentDbConfig, dtoConsumer);
		}
		// 组装最终的展示流：
		// 1. 前置提示 "正在选择合适的数据表..."
		// 2. JSON 起始标记（前端据此识别后续内容是结构化 JSON）
		// 3. LLM 精选的流式输出（schemaFlux）
		// 4. JSON 结束标记
		// 5. 完成提示 "选择数据表完成。"
		return Flux
			.just(ChatResponseUtil.createResponse("正在选择合适的数据表...\n"),
					ChatResponseUtil.createPureResponse(TextType.JSON.getStartSign()))
			.concatWith(schemaFlux)
			.concatWith(Flux.just(ChatResponseUtil.createPureResponse(TextType.JSON.getEndSign()),
					ChatResponseUtil.createResponse("\n\n选择数据表完成。")));
	}

	/**
	 * 获取逻辑外键信息，并过滤只保留与当前召回表相关的外键。
	 *
	 * <p>逻辑外键是用户在管理界面手动配置的表间关联关系，存储在 logical_relation 表中。
	 *    在数仓/大数据平台中，表之间通常没有物理外键约束（因为数据量太大、写入性能等原因），
	 *    但表之间实际存在关联关系（如 order.user_id → user.id）。
	 *    本方法从数据库查询所有逻辑外键，然后过滤出与当前召回表相关的。</p>
	 *
	 * <p>输出格式为 "源表.源列=目标表.目标列"，如 "order_info.user_id=user_profile.id"，
	 *    直接拼接到 LLM Prompt 中，帮助 LLM 理解表间关系并生成正确的 JOIN 语句。</p>
	 *
	 * @param agentId         智能体 ID
	 * @param tableDocuments  向量召回的表文档列表（用于过滤相关外键）
	 * @return 格式化后的逻辑外键列表，异常时返回空列表
	 */
	private List<String> getLogicalForeignKeys(Long agentId, List<Document> tableDocuments) {
		try {
			// 获取当前 agent 激活的数据源，逻辑外键是按数据源维度存储的
			AgentDatasource agentDatasource = agentDatasourceService.getCurrentAgentDatasource(agentId);
			if (agentDatasource == null || agentDatasource.getDatasourceId() == null) {
				log.warn("No active datasource found for agent: {}", agentId);
				return Collections.emptyList();
			}

			Integer datasourceId = agentDatasource.getDatasourceId();

			// 从向量召回的表文档中提取表名集合，用于后续过滤
			Set<String> recalledTableNames = tableDocuments.stream()
				.map(doc -> (String) doc.getMetadata().get("name"))
				.filter(name -> name != null && !name.isEmpty())
				.collect(Collectors.toSet());

			log.info("Recalled table names for agent {}: {}", agentId, recalledTableNames);

			// 查询该数据源下所有用户配置的逻辑外键关系
			List<LogicalRelation> allLogicalRelations = datasourceService.getLogicalRelations(datasourceId);
			log.info("Found {} logical relations in datasource: {}", allLogicalRelations.size(), datasourceId);

			// 过滤逻辑：只保留与当前召回表相关的外键（源表或目标表至少有一个在召回列表中）
			// 这样避免向 LLM 提供不相关表的关系信息，减少干扰
			List<String> formattedForeignKeys = allLogicalRelations.stream()
				.filter(lr -> recalledTableNames.contains(lr.getSourceTableName())
						|| recalledTableNames.contains(lr.getTargetTableName()))
				// 格式化为 "order_info.user_id=user_profile.id" 形式
				.map(lr -> String.format("%s.%s=%s.%s", lr.getSourceTableName(), lr.getSourceColumnName(),
						lr.getTargetTableName(), lr.getTargetColumnName()))
				.distinct()
				.collect(Collectors.toList());

			log.info("Filtered {} relevant logical relations for recalled tables", formattedForeignKeys.size());
			return formattedForeignKeys;
		}
		catch (Exception e) {
			log.error("Error fetching logical foreign keys for agent: {}", agentId, e);
			return Collections.emptyList();
		}
	}

}
