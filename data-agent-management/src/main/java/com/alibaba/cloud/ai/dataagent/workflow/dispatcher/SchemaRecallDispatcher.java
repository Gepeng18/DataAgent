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
package com.alibaba.cloud.ai.dataagent.workflow.dispatcher;

import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;

import java.util.List;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.TABLE_RELATION_NODE;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 表结构召回节点（SchemaRecallNode）的路由分发器。
 * <p>
 * SchemaRecallNode 通过向量相似度检索，从知识库中召回与用户查询相关的数据库表元信息
 * （存储为 Spring AI 的 Document 对象）。本分发器根据召回结果进行路由：
 * <ul>
 *   <li>召回了至少一个表文档 → 路由到 TableRelationNode，分析表间关联关系</li>
 *   <li>未召回任何表文档 → 说明向量知识库中没有与用户查询匹配的表结构，直接结束</li>
 * </ul>
 * <p>
 * 向量知识库中的 Document 通常包含表的 DDL、字段描述、业务术语映射等元信息，
 * 是后续 SQL 生成的重要上下文。
 *
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class SchemaRecallDispatcher implements EdgeAction {

	/**
	 * 根据向量检索召回的表文档数量决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return TABLE_RELATION_NODE（表关系分析）或 END
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		// 从状态中获取 SchemaRecallNode 通过向量检索召回的表结构文档列表
		List<Document> tableDocuments = StateUtil.getDocumentList(state, TABLE_DOCUMENTS_FOR_SCHEMA_OUTPUT);
		if (tableDocuments != null && !tableDocuments.isEmpty())
			return TABLE_RELATION_NODE;
		log.info("No table documents found, ending conversation");
		return END;
	}

}
