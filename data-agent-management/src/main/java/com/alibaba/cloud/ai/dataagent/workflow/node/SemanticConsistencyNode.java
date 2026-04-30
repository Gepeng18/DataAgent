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

import com.alibaba.cloud.ai.dataagent.util.FluxUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import com.alibaba.cloud.ai.dataagent.dto.datasource.SqlRetryDto;
import com.alibaba.cloud.ai.dataagent.dto.prompt.SemanticConsistencyDTO;
import com.alibaba.cloud.ai.dataagent.dto.schema.SchemaDTO;
import com.alibaba.cloud.ai.dataagent.service.nl2sql.Nl2SqlService;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.dataagent.util.PlanProcessUtil.getCurrentExecutionStepInstruction;
import static com.alibaba.cloud.ai.dataagent.prompt.PromptHelper.buildMixMacSqlDbPrompt;

/**
 * 语义一致性校验节点（Semantic Consistency Check）—— SQL 生成后的"Code Review"角色。
 *
 * <p>在工作流中的位置（SQL 分支内部）：
 * <pre>
 *   SqlGenerateNode → <b>SemanticConsistencyNode（本节点）</b> → SqlExecuteNode
 * </pre>
 *
 * <p>核心职责：利用 LLM 对 SqlGenerateNode 生成的 SQL 进行自动化代码审查，包含两个审计维度：
 * <ul>
 *   <li><b>语义一致性</b>：SQL 是否真正完成了当前步骤的执行指令（executionDescription）？
 *       例如用户要求"统计各省份销售额"，SQL 是否确实包含了 GROUP BY 省份和 SUM 销售额？</li>
 *   <li><b>结构正确性</b>：SQL 中引用的字段、表名是否在 Schema 中存在？方言（dialect）是否正确？</li>
 * </ul>
 *
 * <p>涉及的 AI 概念：
 * <ul>
 *   <li><b>LLM-as-Judge</b>：用 LLM 来评估另一个 LLM 生成的输出质量，这是 AI 工程中的常用模式</li>
 *   <li><b>Self-Correction Loop</b>：若校验不通过，会设置 SQL_REGENERATE_REASON 触发 SqlGenerateNode 重新生成 SQL</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see SemanticConsistencyDTO 封装了校验所需的全部上下文信息
 * @see Nl2SqlService#performSemanticConsistency 实际执行语义校验的 LLM 调用
 */
@Slf4j
@Component
@AllArgsConstructor
public class SemanticConsistencyNode implements NodeAction {

	private final Nl2SqlService nl2SqlService;

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到此节点时自动调用。
	 *
	 * <p><b>输入状态读取：</b>
	 * <ul>
	 *   <li>{@code EVIDENCE} — RAG 检索到的相关知识片段</li>
	 *   <li>{@code TABLE_RELATION_OUTPUT} — SchemaRecall + TableRelation 节点筛选的表结构</li>
	 *   <li>{@code DB_DIALECT_TYPE} — 数据库方言类型（如 MySQL、PostgreSQL 等）</li>
	 *   <li>{@code SQL_GENERATE_OUTPUT} — SqlGenerateNode 生成的 SQL 语句</li>
	 *   <li>{@code CANONICAL_QUERY} — 经增强后的用户原始查询</li>
	 *   <li>{@code PLAN_CURRENT_STEP} — 当前执行步骤编号（用于获取对应的 executionDescription）</li>
	 * </ul>
	 *
	 * <p><b>核心逻辑：</b>
	 * <ol>
	 *   <li>组装 {@link SemanticConsistencyDTO}，包含 SQL、Schema、用户查询、执行指令、证据等上下文</li>
	 *   <li>调用 {@link Nl2SqlService#performSemanticConsistency} 让 LLM 审查 SQL</li>
	 *   <li>解析 LLM 返回的审查结果：以"不通过"开头则判定为失败</li>
	 *   <li>将校验结果写入状态，供工作流的条件路由判断是否需要重新生成 SQL</li>
	 * </ol>
	 *
	 * <p><b>输出状态写入：</b>
	 * <ul>
	 *   <li>{@code SEMANTIC_CONSISTENCY_NODE_OUTPUT} — 布尔值，true 表示通过，false 表示不通过</li>
	 *   <li>{@code SQL_REGENERATE_REASON}（仅失败时） — 包含 LLM 给出的修正建议，供 SqlGenerateNode 参考</li>
	 * </ul>
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// 获取 RAG 证据（知识库中的相关文档片段），帮助 LLM 理解业务语义
		String evidence = StateUtil.getStringValue(state, EVIDENCE);
		// 获取数据库表结构信息，用于验证 SQL 中引用的表名、字段名是否合法
		SchemaDTO schemaDTO = StateUtil.getObjectValue(state, TABLE_RELATION_OUTPUT, SchemaDTO.class);
		// 获取数据库方言（MySQL/PostgreSQL/Oracle 等），用于校验 SQL 语法是否符合对应方言
		String dialect = StateUtil.getStringValue(state, DB_DIALECT_TYPE);
		// 获取 SqlGenerateNode 生成的 SQL 语句——这是本次校验的被审查对象
		String sql = StateUtil.getStringValue(state, SQL_GENERATE_OUTPUT);
		// 获取经过增强的用户原始查询，帮助 LLM 理解用户的真实意图
		String userQuery = StateUtil.getCanonicalQuery(state);

		// 组装语义一致性校验所需的全部上下文：
		// - sql: 待校验的 SQL
		// - executionDescription: 当前步骤的执行指令（来自 Plan），用于判断 SQL 是否完成了该步骤的任务
		// - schemaInfo: 数据库表结构，用于检查字段/表名引用合法性
		// - userQuery: 用户原始问题
		// - evidence: RAG 检索的证据
		SemanticConsistencyDTO semanticConsistencyDTO = SemanticConsistencyDTO.builder()
			.dialect(dialect)
			.sql(sql)
			.executionDescription(getCurrentExecutionStepInstruction(state))
			.schemaInfo(buildMixMacSqlDbPrompt(schemaDTO, true))
			.userQuery(userQuery)
			.evidence(evidence)
			.build();

		// LLM 做 Code Review：两个审计维度——语义一致性（SQL是否完成步骤指令）和结构正确性（字段是否存在、方言是否正确）
		log.info("Starting semantic consistency validation - SQL: {}", sql);
		Flux<ChatResponse> validationResultFlux = nl2SqlService.performSemanticConsistency(semanticConsistencyDTO);

		// 将 LLM 返回的流式审查结果包装为 StreamingOutput，同时解析审查结论：
		// - LLM 返回以"不通过"开头 → 校验失败，需要重新生成 SQL
		// - 其他情况 → 校验通过，流程继续到 SqlExecuteNode
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGeneratorWithMessages(this.getClass(),
				state, "开始语义一致性校验", "语义一致性校验完成", validationResult -> {
					boolean isPassed = !validationResult.startsWith("不通过");
					Map<String, Object> result = buildValidationResult(isPassed, validationResult);
					log.info("[{}] Semantic consistency validation result: {}, passed: {}",
							this.getClass().getSimpleName(), validationResult, isPassed);
					return result;
				}, validationResultFlux);

		return Map.of(SEMANTIC_CONSISTENCY_NODE_OUTPUT, generator);
	}

	/**
	 * 根据校验结果构建状态更新。
	 *
	 * <p>校验通过时仅设置布尔标记；校验失败时还会附带 {@link SqlRetryDto#semantic(String)} 封装的修正建议。
	 * 该修正建议会在下一次 SqlGenerateNode 调用时作为上下文传入，帮助 LLM 避免同样的错误——
	 * 这构成了一个 "Self-Correction Loop"（自纠正循环）。
	 *
	 * @param passed 校验是否通过
	 * @param validationResult LLM 返回的审查结论文本
	 * @return 状态更新 Map
	 */
	private Map<String, Object> buildValidationResult(boolean passed, String validationResult) {
		if (passed) {
			return Map.of(SEMANTIC_CONSISTENCY_NODE_OUTPUT, true);
		}
		else {
			return Map.of(SEMANTIC_CONSISTENCY_NODE_OUTPUT, false, SQL_REGENERATE_REASON,
					SqlRetryDto.semantic(validationResult));
		}
	}

}
