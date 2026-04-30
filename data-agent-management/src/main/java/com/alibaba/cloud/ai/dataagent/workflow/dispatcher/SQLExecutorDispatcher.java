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

import com.alibaba.cloud.ai.dataagent.dto.datasource.SqlRetryDto;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * SQL 执行节点（SqlExecuteNode）的路由分发器。
 * <p>
 * SqlExecuteNode 负责将 LLM 生成的 SQL 语句提交到数据库执行。执行可能因各种原因
 * 失败（SQL 语法错误、权限不足、表不存在等），因此本分发器根据执行结果进行路由：
 * <ul>
 *   <li>执行失败 → 路由回 SqlGenerateNode，携带失败原因让 LLM 修正 SQL 后重新生成</li>
 *   <li>执行成功 → 路由回 PlanExecutorNode，由计划执行器继续推进下一个执行步骤</li>
 * </ul>
 * <p>
 * 注意：失败原因通过 SqlRetryDto 承载，其中 sqlExecuteFail() 区分了"执行失败"
 * 和"语义校验失败"两种不同的重试场景。
 *
 * @author zhangshenghang
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 * @see com.alibaba.cloud.ai.dataagent.dto.datasource.SqlRetryDto
 */
@Slf4j
public class SQLExecutorDispatcher implements EdgeAction {

	/**
	 * 根据 SQL 执行结果决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return SQL_GENERATE_NODE（执行失败，重新生成SQL）或 PLAN_EXECUTOR_NODE（执行成功，继续计划）
	 */
	@Override
	public String apply(OverAllState state) {
		// SqlRetryDto 封装了 SQL 重试的原因，包含语义失败和执行失败两种标记
		SqlRetryDto retryDto = StateUtil.getObjectValue(state, SQL_REGENERATE_REASON, SqlRetryDto.class);
		if (retryDto.sqlExecuteFail()) {
			// SQL 在数据库执行时出错，需要让 LLM 根据错误信息修正 SQL
			log.warn("SQL运行失败，需要重新生成！");
			return SQL_GENERATE_NODE;
		}
		else {
			// SQL 执行成功，返回计划执行器处理下一个步骤
			log.info("SQL运行成功，返回PlanExecutorNode。");
			return PLAN_EXECUTOR_NODE;
		}
	}

}
