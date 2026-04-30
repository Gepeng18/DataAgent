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

import java.util.Optional;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 表关系分析节点（TableRelationNode）的路由分发器。
 * <p>
 * TableRelationNode 负责 LLM 分析已召回表之间的关联关系（JOIN 关系、外键等）。
 * 该节点可能因为 LLM 输出格式异常等原因失败，因此本分发器实现了"可重试错误"机制：
 * <ul>
 *   <li>如果节点设置了异常标记且错误以 "RETRYABLE:" 前缀开头，且重试次数未超上限，
 *       则路由回 TableRelationNode 重新执行</li>
 *   <li>如果错误不可重试或已达最大重试次数，直接结束流程</li>
 *   <li>如果无异常且成功输出了表关系分析结果，路由到 FeasibilityAssessmentNode（可行性评估）</li>
 * </ul>
 *
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
public class TableRelationDispatcher implements EdgeAction {

	/** 表关系分析节点的最大重试次数 */
	private static final int MAX_RETRY_COUNT = 3;

	/**
	 * 根据表关系分析的执行结果和错误状态决定下一个节点。
	 * <p>
	 * 路由优先级：错误重试 > 成功输出 > 无输出结束
	 *
	 * @param state 工作流全局状态对象
	 * @return TABLE_RELATION_NODE（重试）、FEASIBILITY_ASSESSMENT_NODE 或 END
	 */
	@Override
	public String apply(OverAllState state) throws Exception {

		// 检查 TableRelationNode 是否设置了异常标记
		String errorFlag = StateUtil.getStringValue(state, TABLE_RELATION_EXCEPTION_OUTPUT, null);
		Integer retryCount = StateUtil.getObjectValue(state, TABLE_RELATION_RETRY_COUNT, Integer.class, 0);

		if (errorFlag != null && !errorFlag.isEmpty()) {
			// 可重试错误且未达上限时，路由回自身重试
			if (isRetryableError(errorFlag) && retryCount < MAX_RETRY_COUNT) {
				return TABLE_RELATION_NODE;
			}
			else {
				// 不可重试或已达上限，终止流程
				return END;
			}
		}

		// 无异常，检查是否有正常的表关系分析输出
		Optional<String> tableRelationOutput = state.value(TABLE_RELATION_OUTPUT);
		if (tableRelationOutput.isPresent()) {
			return FEASIBILITY_ASSESSMENT_NODE;
		}

		// 既无异常也无输出，异常状态，直接结束
		return END;
	}

	/**
	 * 判断错误是否可重试。
	 * <p>
	 * 约定：节点在设置异常标记时，如果错误以 "RETRYABLE:" 前缀开头，表示该错误
	 * 可以通过重新调用 LLM 来恢复（例如输出格式不合规但并非逻辑性错误）。
	 */
	private boolean isRetryableError(String errorMessage) {
		return errorMessage.startsWith("RETRYABLE:");
	}

}
