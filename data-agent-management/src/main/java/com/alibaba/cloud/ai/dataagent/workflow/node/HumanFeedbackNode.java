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

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 人工审核反馈节点 —— StateGraph工作流中实现"Human-in-the-Loop"（人机协作）的关键节点。
 *
 * <h3>工作流位置</h3>
 * <p>位于 {@code PlannerNode}（计划生成）之后、{@code PlanExecutorNode}（计划执行）之前。
 * 当用户在请求中设置 {@code humanFeedback=true} 时，StateGraph 框架通过
 * {@code CompiledGraph.interruptBefore(HUMAN_FEEDBACK_NODE)} 机制在进入本节点前暂停工作流，
 * 等待用户在前端界面审核执行计划并给出批准或修改意见后，再恢复执行。</p>
 *
 * <h3>核心职责</h3>
 * <ul>
 *   <li><b>计划审核</b>：将 LLM 生成的执行计划展示给用户，由用户决定是否批准</li>
 *   <li><b>反馈路由</b>：根据用户反馈（批准/拒绝）决定工作流下一步走向：
 *     <ul>
 *       <li>批准 → 路由到 {@code PlanExecutorNode} 开始执行计划</li>
 *       <li>拒绝 → 路由回 {@code PlannerNode} 重新生成计划，并将用户修改意见注入 Prompt</li>
 *     </ul>
 *   </li>
 *   <li><b>重试保护</b>：最多允许3次修改循环，防止无限重试</li>
 * </ul>
 *
 * <h3>涉及的AI/工程概念</h3>
 * <ul>
 *   <li><b>Human-in-the-Loop（HITL）</b>：AI Agent设计中的一种模式，在关键决策点暂停自动化流程，
 *       等待人类确认后再继续。本项目中通过 StateGraph 的 {@code interruptBefore} 机制实现。</li>
 *   <li><b>StateGraph 中断恢复</b>：框架在配置了 {@code interruptBefore(HUMAN_FEEDBACK_NODE)} 后，
 *       会在进入本节点前暂停图执行。用户通过 SSE 接口提交反馈后，框架用相同的 threadId 恢复执行，
 *       本节点的 apply 方法在恢复后被调用，根据反馈数据决定后续路由。</li>
 *   <li><b>条件路由</b>：通过设置 {@code human_next_node} 状态字段，告知 PlanExecutor 下一步
 *       应该跳转到哪个节点，实现基于用户反馈的动态路由。</li>
 * </ul>
 *
 * <h3>状态读取与写入</h3>
 * <ul>
 *   <li><b>读取</b>：{@code HUMAN_FEEDBACK_DATA}（用户提交的反馈数据，Map结构）、
 *       {@code PLAN_REPAIR_COUNT}（已修改次数）</li>
 *   <li><b>写入</b>：{@code human_next_node}（路由目标节点名称）、{@code HUMAN_REVIEW_ENABLED}（是否继续人工审核）、
 *       {@code PLAN_REPAIR_COUNT}（更新修改次数）、{@code PLAN_VALIDATION_ERROR}（用户修改意见，注入Planner重新生成）、
 *       {@code PLANNER_NODE_OUTPUT}（清空旧计划，触发重新生成）</li>
 * </ul>
 *
 * @author Makoto
 * @see com.alibaba.cloud.ai.dataagent.controller.GraphController#streamSearch SSE端点，处理中断恢复请求
 */
@Slf4j
@Component
public class HumanFeedbackNode implements NodeAction {

	/**
	 * 节点执行入口，由 StateGraph 框架在中断恢复后自动调用。
	 *
	 * <p><b>执行时机</b>：当配置了 {@code interruptBefore(HUMAN_FEEDBACK_NODE)} 时，框架在进入本节点前
	 * 暂停工作流执行。用户在前端审核计划并提交反馈后，框架通过 SSE 接口恢复执行，
	 * 此时本方法被调用，从状态中读取用户反馈数据并决定后续路由。</p>
	 *
	 * <p><b>路由逻辑</b>：</p>
	 * <ul>
	 *   <li>修改次数 >= 3 → 终止流程（END）</li>
	 *   <li>无反馈数据 → 设置为等待状态（WAIT_FOR_FEEDBACK）</li>
	 *   <li>用户批准 → 路由到 PlanExecutorNode 开始执行</li>
	 *   <li>用户拒绝 → 路由回 PlannerNode 重新生成计划，用户反馈作为修改指导</li>
	 * </ul>
	 *
	 * @param state 当前工作流的全局共享状态对象
	 * @return 需要更新到状态中的字段映射，控制后续节点路由
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {
		Map<String, Object> updated = new HashMap<>();

		// ---- 1. 重试次数保护 ----
		// 防止用户反复拒绝导致无限循环，最多允许3次修改计划
		int repairCount = StateUtil.getObjectValue(state, PLAN_REPAIR_COUNT, Integer.class, 0);
		if (repairCount >= 3) {
			log.warn("Max repair attempts (3) exceeded, ending process");
			updated.put("human_next_node", "END");
			return updated;
		}

		// ---- 2. 读取用户反馈数据 ----
		// HUMAN_FEEDBACK_DATA 是用户通过前端提交的反馈，结构为 Map，包含：
		//   - "feedback": Boolean 值，true=批准，false=拒绝
		//   - "feedback_content": String，用户的修改意见（仅拒绝时有意义）
		// 如果为空，说明用户还未提交反馈，工作流应保持等待状态
		Map<String, Object> feedbackData = StateUtil.getObjectValue(state, HUMAN_FEEDBACK_DATA, Map.class, Map.of());
		if (feedbackData.isEmpty()) {
			updated.put("human_next_node", "WAIT_FOR_FEEDBACK");
			return updated;
		}

		// ---- 3. 解析用户反馈：批准 or 拒绝 ----
		// 注意：feedback 字段可能是 Boolean 类型（正常情况）或 String 类型（某些序列化场景），需要兼容处理
		Object approvedValue = feedbackData.getOrDefault("feedback", true);
		boolean approved = approvedValue instanceof Boolean approvedBoolean ? approvedBoolean
				: Boolean.parseBoolean(approvedValue.toString());

		if (approved) {
			// ---- 3a. 用户批准：路由到 PlanExecutorNode 执行计划 ----
			log.info("Plan approved → execution");
			updated.put("human_next_node", PLAN_EXECUTOR_NODE);
			// 关闭人工审核标记，后续步骤不再需要人工确认
			updated.put(HUMAN_REVIEW_ENABLED, false);
		}
		else {
			// ---- 3b. 用户拒绝：路由回 PlannerNode 重新生成计划 ----
			log.info("Plan rejected → regeneration (attempt {})", repairCount + 1);
			updated.put("human_next_node", PLANNER_NODE);
			updated.put(PLAN_REPAIR_COUNT, repairCount + 1);
			// 重置步骤编号为1，让 Planner 重新从头规划
			updated.put(PLAN_CURRENT_STEP, 1);
			// 保持人工审核开启，新计划仍需用户确认
			updated.put(HUMAN_REVIEW_ENABLED, true);

			// 将用户的修改意见写入 PLAN_VALIDATION_ERROR 字段
			// PlannerNode 在重新生成时会读取此字段，将用户意见作为约束条件注入 Prompt
			String feedbackContent = feedbackData.getOrDefault("feedback_content", "").toString();
			updated.put(PLAN_VALIDATION_ERROR,
					StringUtils.hasLength(feedbackContent) ? feedbackContent : "Plan rejected by user");
			// 清空旧计划输出，触发 PlannerNode 重新调用 LLM 生成新计划
			updated.put(PLANNER_NODE_OUTPUT, "");
		}

		return updated;
	}

}
