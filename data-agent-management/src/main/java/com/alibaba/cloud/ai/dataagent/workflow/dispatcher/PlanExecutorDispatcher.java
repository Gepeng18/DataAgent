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

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 计划执行节点（PlanExecutorNode）的路由分发器。
 * <p>
 * PlanExecutorNode 是整个工作流中的核心编排节点——它按照 PlannerNode 生成的执行计划，
 * 逐步调度 SQL 生成/执行、Python 代码生成/执行等子步骤。本分发器负责在每一步执行后
 * 决定下一步的去向，包含两条主要路径：
 * <ol>
 *   <li><b>计划验证通过</b>：从状态中读取 PLAN_NEXT_NODE（由 PlanExecutorNode 设置的
 *       下一步节点名），路由到对应的子节点（如 SQL_GENERATE_NODE、PYTHON_GENERATE_NODE 等）。
 *       如果 PLAN_NEXT_NODE 为 "END"，表示所有步骤已完成。</li>
 *   <li><b>计划验证失败</b>：路由回 PlannerNode 让 LLM 重新生成/修复执行计划。
 *       设有最大修复次数限制（MAX_REPAIR_ATTEMPTS），超过后直接终止流程。</li>
 * </ol>
 *
 * @author zhangshenghang
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class PlanExecutorDispatcher implements EdgeAction {

	/** 计划修复的最大尝试次数，超过后不再重试直接结束 */
	private static final int MAX_REPAIR_ATTEMPTS = 2;

	/**
	 * 根据计划执行状态决定下一步节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return PLAN_NEXT_NODE 指定的下一个节点、PLANNER_NODE（计划修复）或 END
	 */
	@Override
	public String apply(OverAllState state) {
		// 检查 PlanExecutorNode 是否通过了当前步骤的验证
		boolean validationPassed = StateUtil.getObjectValue(state, PLAN_VALIDATION_STATUS, Boolean.class, false);

		if (validationPassed) {
			log.info("Plan validation passed. Proceeding to next step.");
			// PlanExecutorNode 在状态中预设了下一步要执行的节点名称
			String nextNode = state.value(PLAN_NEXT_NODE, END);
			// 字符串 "END" 是节点约定的结束标记，需转换为 StateGraph 的 END 常量
			if ("END".equals(nextNode)) {
				log.info("Plan execution completed successfully.");
				return END;
			}
			return nextNode;
		}
		else {
			// 计划验证失败，检查修复次数是否已达上限
			int repairCount = StateUtil.getObjectValue(state, PLAN_REPAIR_COUNT, Integer.class, 0);

			if (repairCount > MAX_REPAIR_ATTEMPTS) {
				log.error("Plan repair attempts exceeded the limit of {}. Terminating execution.", MAX_REPAIR_ATTEMPTS);
				return END;
			}

			log.warn("Plan validation failed. Routing back to PlannerNode for repair. Attempt count from state: {}.",
					repairCount);
			return PLANNER_NODE;
		}
	}

}
