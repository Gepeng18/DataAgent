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

import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 人工反馈节点（HumanFeedbackNode）的路由分发器。
 * <p>
 * 这是"Human-in-the-Loop"（人机协作）模式的核心路由组件。当工作流执行到人工反馈环节时，
 * 系统会暂停执行并等待用户确认或修改执行计划。本分发器处理两种场景：
 * <ul>
 *   <li><b>等待反馈（WAIT_FOR_FEEDBACK）</b>：返回 END 使 StateGraph 暂停执行，
 *       但这不是真正的"结束"——外部调用方会通过 threadId 和反馈数据重新恢复工作流，
 *       恢复时 HumanFeedbackNode 会设置 human_next_node 为实际要执行的下一个节点</li>
 *   <li><b>已有反馈</b>：直接返回 human_next_node 中指定的节点名称，继续执行工作流</li>
 * </ul>
 * <p>
 * 技术实现上，StateGraph 的 interruptBefore 机制会配合 END 返回值实现图的中断暂停，
 * 后续通过 CompiledGraph 的 resume 操作恢复执行。
 *
 * @author Makoto
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
public class HumanFeedbackDispatcher implements EdgeAction {

	/**
	 * 根据人工反馈状态决定下一个节点或暂停工作流。
	 *
	 * @param state 工作流全局状态对象
	 * @return END（暂停等待反馈）或下一个要执行的节点名称
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		// human_next_node 由 HumanFeedbackNode 在处理用户反馈后设置
		String nextNode = (String) state.value("human_next_node", END);

		// WAIT_FOR_FEEDBACK 是暂停标记：此时返回 END 使 StateGraph 停止执行，
		// 等待前端通过 SSE 重新提交用户反馈后恢复
		if ("WAIT_FOR_FEEDBACK".equals(nextNode)) {
			return END;
		}

		return nextNode;
	}

}
