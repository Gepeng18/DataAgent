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

import com.alibaba.cloud.ai.dataagent.dto.prompt.QueryEnhanceOutputDTO;
import com.alibaba.cloud.ai.dataagent.mapper.AgentDatasourceMapper;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.dataagent.service.schema.SchemaService;
import com.alibaba.cloud.ai.dataagent.util.ChatResponseUtil;
import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * Schema 召回节点 —— StateGraph 工作流中负责从向量数据库检索相关数据库表和列信息的节点。
 *
 * <h3>在工作流中的位置</h3>
 * <p>... → EvidenceRecallNode → QueryEnhance → <b>SchemaRecallNode（当前节点）</b> → TableRelationNode → ...</p>
 *
 * <h3>核心职责</h3>
 * <p>根据用户提问（经过 QueryEnhance 节点改写后的 canonicalQuery），
 *    在向量数据库中检索最相关的数据库表（Table）和列（Column）的 Schema 信息，
 *    为后续 TableRelationNode 进行表关系推断和 SQL 生成提供结构化的上下文。</p>
 *
 * <h3>涉及的 AI 概念</h3>
 * <ul>
 *   <li><b>Schema Linking（模式链接）</b>：NL2SQL 任务中的关键步骤。
 *       将自然语言问题映射到数据库的具体表名和列名，是 Text-to-SQL 的前置环节。
 *       传统方法依赖精确匹配或规则，这里利用向量语义检索实现模糊匹配。</li>
 *   <li><b>Embedding / 向量检索</b>：数据库的表结构信息（表名、列名、注释、DDL 等）
 *       在初始化时已经过 Embedding 模型转换为高维向量存入 VectorStore。
 *       检索时将用户问题也转为向量，通过余弦相似度找出最相关的 TopK 表和列。
 *       这一步由 {@link SchemaService} 封装完成，本节点不直接调用 Embedding 模型。</li>
 *   <li><b>两阶段检索策略</b>：先检索相关表（粗筛），再在找到的表范围内检索相关列（精筛）。
 *       类似搜索引擎中"先找文档再找段落"的策略，避免在全量列空间中搜索引入噪音。</li>
 *   <li><b>纯向量检索（不调用 LLM）</b>：本节点只做向量检索，不调用 LLM。
 *       与 EvidenceRecallNode 不同，这里无需查询重写，因为输入已经过 QueryEnhance 节点优化。</li>
 * </ul>
 *
 * <h3>处理流程</h3>
 * <ol>
 *   <li>从全局状态读取经 QueryEnhance 改写后的 canonicalQuery 和 Agent ID</li>
 *   <li>查询当前 Agent 激活的数据源（Datasource），确定检索范围</li>
 *   <li><b>第一阶段 - 表检索</b>：用 canonicalQuery 在向量库中检索相关的表文档</li>
 *   <li><b>第二阶段 - 列检索</b>：在找到的表范围内，检索相关的列文档</li>
 *   <li>将表文档和列文档写入全局状态，供下游 TableRelationNode 使用</li>
 * </ol>
 *
 * <h3>输出说明</h3>
 * <p>输出的 Document 中，metadata 包含数据库元信息：</p>
 * <ul>
 *   <li>表文档 metadata：name=表名, comment=表注释, ddl=建表语句 等</li>
 *   <li>列文档 metadata：name=列名, tableName=所属表名, type=数据类型, comment=列注释 等</li>
 * </ul>
 *
 * @see SchemaService Schema 召回服务（封装了向量检索逻辑）
 * @see NodeAction StateGraph 节点的统一接口
 * @see com.alibaba.cloud.ai.dataagent.dto.prompt.QueryEnhanceOutputDTO QueryEnhance 节点的输出结构
 * @author zhangshenghang
 */
@Slf4j
@Component
@AllArgsConstructor
public class SchemaRecallNode implements NodeAction {

	private final SchemaService schemaService;

	private final AgentDatasourceMapper agentDatasourceMapper;

	/**
	 * 节点执行入口，由 StateGraph 框架在到达该节点时自动调用。
	 *
	 * <h3>输入状态读取</h3>
	 * <ul>
	 *   <li>{@code QUERY_ENHANCE_NODE_OUTPUT} —— QueryEnhance 节点的输出 DTO，
	 *       包含 canonicalQuery（经改写后的标准查询语句）</li>
	 *   <li>{@code AGENT_ID} —— 当前智能体 ID，用于查询其关联的数据源</li>
	 * </ul>
	 *
	 * <h3>检索逻辑（不调用 LLM，纯向量检索）</h3>
	 * <ol>
	 *   <li>通过 agentId 查询当前 Agent 激活的数据源 ID</li>
	 *   <li><b>第一阶段</b>：以 canonicalQuery 为查询条件，在向量库中检索相关表文档</li>
	 *   <li><b>第二阶段</b>：从表文档中提取表名，在这些表的范围内检索相关列文档</li>
	 * </ol>
	 *
	 * <h3>输出状态写入</h3>
	 * <ul>
	 *   <li>{@code SCHEMA_RECALL_NODE_OUTPUT} —— 流式生成器（包装检索进度消息）</li>
	 *   <li>{@code TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT} —— 检索到的表文档列表，
	 *       供 TableRelationNode 构建初始 Schema 使用</li>
	 *   <li>{@code COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT} —— 检索到的列文档列表，
	 *       供 TableRelationNode 在 LLM 精选时作为候选列使用</li>
	 * </ul>
	 *
	 * @param state StateGraph 的全局共享状态对象
	 * @return 包含流式生成器和 Schema 文档的 Map
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// 从全局状态中读取 QueryEnhance 节点的输出，提取改写后的标准查询语句
		// canonicalQuery 是经过同义词扩展、指代消解后的标准化查询，比用户原始输入更适合向量检索
		QueryEnhanceOutputDTO queryEnhanceOutputDTO = StateUtil.getObjectValue(state, QUERY_ENHANCE_NODE_OUTPUT,
				QueryEnhanceOutputDTO.class);
		String input = queryEnhanceOutputDTO.getCanonicalQuery();
		String agentId = StateUtil.getStringValue(state, AGENT_ID);

		// 查询当前 Agent 激活的数据源 ID。
		// 一个 Agent 可以关联多个数据源，但同一时刻只有一个处于激活状态。
		// 数据源决定了向量检索的范围——每个数据源有独立的 Schema 向量空间。
		Integer datasourceId = agentDatasourceMapper.selectActiveDatasourceIdByAgentId(Long.valueOf(agentId));

		if (datasourceId == null) {
			log.warn("Agent {} has no active datasource", agentId);
			// 没有激活的数据源时，向前端推送错误提示并返回空文档列表，终止后续流程
			String noDataSourceMessage = """
					\n 该智能体没有激活的数据源

					这可能是因为：
					1. 数据源尚未配置或关联。
					2. 所有数据源都已被禁用。
					3. 请先配置并激活数据源。
					流程已终止。
					""";

			// Flux.create 手动构建一个 Flux 流，将错误提示消息推送到前端
				Flux<ChatResponse> displayFlux = Flux.create(emitter -> {
					emitter.next(ChatResponseUtil.createResponse(noDataSourceMessage));
					emitter.complete();
				});

				// createStreamingGeneratorWithMessages 创建流式生成器：
				// displayFlux 负责向前端推送进度消息，resultMapper 回调负责写入全局状态。
				// 这里返回空的文档列表，表示没有可用的 Schema 信息。
				Flux<GraphResponse<StreamingOutput>> generator = FluxUtil
					.createStreamingGeneratorWithMessages(this.getClass(), state, currentState -> {
						return Map.of(TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT, Collections.emptyList(),
								COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT, Collections.emptyList());
					}, displayFlux);

				return Map.of(SCHEMA_RECALL_NODE_OUTPUT, generator);
			}

			// ============ 两阶段向量检索（不调用 LLM） ============

			// 第一阶段：表级别检索。
			// schemaService.getTableDocumentsByDatasource 内部会将 canonicalQuery 通过 Embedding 模型转为向量，
			// 然后在指定数据源的向量空间中按余弦相似度检索 TopK 个最相关的表文档。
			// 每个 Document 的 text 包含表的 DDL/注释，metadata 包含表名等元信息。
			List<Document> tableDocuments = new ArrayList<>(
					schemaService.getTableDocumentsByDatasource(datasourceId, input));

			// 从表文档的 metadata 中提取表名列表，作为第二阶段列检索的过滤条件
			List<String> recalledTableNames = extractTableName(tableDocuments);

			// 第二阶段：列级别检索（精筛）。
			// 在第一阶段找到的表范围内，进一步检索与查询相关的列文档。
			// 这样避免在全量列空间中搜索，减少噪音，提高后续 SQL 生成节点的准确率。
			List<Document> columnDocuments = schemaService.getColumnDocumentsByTableName(datasourceId, recalledTableNames);

		// 构建检索失败的提示消息。注意第 4 点：
		// 如果数据源初始化时用的是 A 嵌入模型（如 text-embedding-ada-002），
		// 后来系统切换为 B 嵌入模型（如 bge-large-zh），两者的向量空间不兼容，
		// 需要重新初始化数据源（重新做 Embedding 入库），否则检索结果会不准确。
		String failMessage = """
				\n 未检索到相关数据表

				这可能是因为：
				1. 数据源尚未初始化。
				2. 您的提问与当前数据库中的表结构无关。
				3. 请尝试点击“初始化数据源”或换一个与业务相关的问题。
				4. 如果你用A嵌入模型初始化数据源，却更换为B嵌入模型，请重新初始化数据源
				流程已终止。
				""";

		// 构建展示用 Flux 流：向前端推送 Schema 召回的进度信息。
		// Flux.create 是 Reactor 的手动发射器模式，emitter.next() 推送一条消息，emitter.complete() 结束流。
		Flux<ChatResponse> displayFlux = Flux.create(emitter -> {
			emitter.next(ChatResponseUtil.createResponse("开始初步召回Schema信息..."));
			emitter.next(ChatResponseUtil.createResponse(
					"初步表信息召回完成，数量: " + tableDocuments.size() + "，表名: " + String.join(", ", recalledTableNames)));
			if (tableDocuments.isEmpty()) {
				// 检索结果为空时，推送失败提示
				emitter.next(ChatResponseUtil.createResponse(failMessage));
			}
			emitter.next(ChatResponseUtil.createResponse("初步Schema信息召回完成."));
			emitter.complete();
		});

		// createStreamingGeneratorWithMessages 将 displayFlux（前端展示流）和 resultMapper（状态写入回调）组合，
		// 返回一个 Flux<GraphResponse<StreamingOutput>> 供框架消费。
		// resultMapper 中的 currentState -> Map.of(...) 会在流消费完毕后被调用，
		// 将表文档和列文档写入全局状态。
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, currentState -> {
					return Map.of(TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT, tableDocuments,
							COLUMN_DOCUMENTS__FOR_SCHEMA_OUTPUT, columnDocuments);
				}, displayFlux);

		// 将生成器写入全局状态，key 为 SCHEMA_RECALL_NODE_OUTPUT
		return Map.of(SCHEMA_RECALL_NODE_OUTPUT, generator);
	}

	/**
	 * 从表文档列表的 metadata 中提取表名。
	 *
	 * <p>向量检索返回的 Document 对象，其 metadata 中存储了数据库元信息。
	 * 表文档的 metadata.name 字段即为数据库中的表名（如 "order_info"、"user_profile" 等）。</p>
	 *
	 * @param tableDocuments 向量检索返回的表文档列表
	 * @return 表名列表，用于第二阶段列检索的范围限定
	 */
	private static List<String> extractTableName(List<Document> tableDocuments) {
		List<String> tableNames = new ArrayList<>();
		for (Document document : tableDocuments) {
			// 从 Document 的 metadata 中提取 name 字段（即数据库表名）
			String name = (String) document.getMetadata().get("name");
			if (name != null && !name.isEmpty()) {
				tableNames.add(name);
			}
		}
		log.info("At this SchemaRecallNode, Recall tables are: {}", tableNames);
		return tableNames;

	}

}
