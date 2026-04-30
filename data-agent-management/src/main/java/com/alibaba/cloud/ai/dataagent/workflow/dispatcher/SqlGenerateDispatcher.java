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

import com.alibaba.cloud.ai.dataagent.properties.DataAgentProperties;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * SQL 生成节点（SqlGenerateNode）的路由分发器。
 * <p>
 * SqlGenerateNode 由 LLM 根据用户查询和召回的表结构信息生成 SQL 语句。
 * 本分发器处理三种情况：
 * <ol>
 *   <li><b>生成失败（输出为空）</b>：重试机制，重新路由回 SqlGenerateNode 让 LLM 重新生成，
 *       直到达到最大重试次数（由配置项 maxSqlRetryCount 控制）</li>
 *   <li><b>LLM 主动返回 END 标记</b>：LLM 判断无法生成有效 SQL（如表结构不足以回答用户问题），
 *       此时直接结束流程</li>
 *   <li><b>生成成功</b>：路由到 SemanticConsistencyNode，对生成的 SQL 进行语义一致性校验
 *       （验证 SQL 是否真正对应用户的查询意图）</li>
 * </ol>
 * <p>
 * 注意：本类通过 Spring 注入 DataAgentProperties 以读取最大重试次数等配置。
 *
 * @author zhangshenghang
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
@Component
@AllArgsConstructor
public class SqlGenerateDispatcher implements EdgeAction {

	private final DataAgentProperties properties;

	/**
	 * 根据 SQL 生成结果决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return SQL_GENERATE_NODE（重试）、SEMANTIC_CONSISTENCY_NODE（语义检查）或 END
	 */
	@Override
	public String apply(OverAllState state) {
		Optional<Object> optional = state.value(SQL_GENERATE_OUTPUT);
		if (optional.isEmpty()) {
			// SQL 生成失败（LLM 未返回有效输出），检查是否可以重试
			int currentCount = state.value(SQL_GENERATE_COUNT, properties.getMaxSqlRetryCount());
			if (currentCount < properties.getMaxSqlRetryCount()) {
				log.info("SQL 生成失败，开始重试，当前次数: {}", currentCount);
				return SQL_GENERATE_NODE;
			}
			log.error("SQL 生成失败，达到最大重试次数，结束流程");
			return END;
		}
		String sqlGenerateOutput = (String) optional.get();
		log.info("SQL 生成结果: {}", sqlGenerateOutput);

		// LLM 可能返回 "END" 字符串，表示其判断无法生成有意义的 SQL
		if (END.equals(sqlGenerateOutput)) {
			log.info("检测到流程结束标志: {}", END);
			return END;
		}
		else {
			// SQL 生成成功，进入语义一致性校验
			log.info("SQL生成成功，进入语义一致性检查节点: {}", SEMANTIC_CONSISTENCY_NODE);
			return SEMANTIC_CONSISTENCY_NODE;
		}
	}

}
