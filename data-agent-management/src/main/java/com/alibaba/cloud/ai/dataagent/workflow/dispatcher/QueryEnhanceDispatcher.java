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

import com.alibaba.cloud.ai.dataagent.dto.prompt.QueryEnhanceOutputDTO;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.QUERY_ENHANCE_NODE_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.SCHEMA_RECALL_NODE;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 查询增强节点（QueryEnhanceNode）的路由分发器。
 * <p>
 * 查询增强是 RAG（检索增强生成）流水线中的关键环节：将用户的自然语言查询改写为更规范的
 * "标准查询"（canonicalQuery），同时生成多个"扩展查询"（expandedQueries）以提高召回率。
 * <p>
 * 本分发器的路由逻辑：
 * <ul>
 *   <li>如果 LLM 未能生成有效的标准查询或扩展查询，说明查询改写失败，直接结束流程</li>
 *   <li>如果改写成功，路由到 SchemaRecallNode（表结构召回），利用扩展查询从向量数据库中
 *       检索最相关的数据库表/字段元信息</li>
 * </ul>
 *
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class QueryEnhanceDispatcher implements EdgeAction {

	/**
	 * 根据查询增强的输出结果决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return SCHEMA_RECALL_NODE（表结构召回）或 END
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		QueryEnhanceOutputDTO queryProcessOutput = StateUtil.getObjectValue(state, QUERY_ENHANCE_NODE_OUTPUT,
				QueryEnhanceOutputDTO.class);

		// LLM 未返回查询改写结果，无法继续
		if (queryProcessOutput == null) {
			log.warn("Query process output is null, ending conversation");
			return END;
		}

		// canonicalQuery：用户查询的规范化版本（例如去除歧义、补全省略信息）
		// expandedQueries：基于原始查询生成的多个同义/相关查询，用于提高向量检索的召回率
		boolean isCanonicalQueryEmpty = queryProcessOutput.getCanonicalQuery() == null
				|| queryProcessOutput.getCanonicalQuery().trim().isEmpty();
		boolean isExpandedQueriesEmpty = queryProcessOutput.getExpandedQueries() == null
				|| queryProcessOutput.getExpandedQueries().isEmpty();

		if (isCanonicalQueryEmpty || isExpandedQueriesEmpty) {
			log.warn("Query process output contains empty fields - canonicalQuery: {}, expandedQueries: {}",
					isCanonicalQueryEmpty, isExpandedQueriesEmpty);
			return END;
		}
		else {
			log.info("Query process output is valid, proceeding to schema recall");
			return SCHEMA_RECALL_NODE;
		}
	}

}
