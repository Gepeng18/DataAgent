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
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.dataagent.dto.planner.ExecutionStep;
import com.alibaba.cloud.ai.dataagent.dto.planner.Plan;
import com.alibaba.cloud.ai.dataagent.util.PlanProcessUtil;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 计划执行调度节点（PlanExecutor）—— StateGraph 工作流中的"项目经理"角色。
 *
 * <p>在工作流中的位置：Planner → [HumanFeedback] → <b>PlanExecutor（本节点）</b>
 *
 * <p>本节点本身不执行任何业务逻辑，只负责三件事：
 * <ol>
 *   <li><b>计划验证</b>：解析 Planner 生成的 Plan JSON，校验其结构完整性和每一步的工具/参数合法性</li>
 *   <li><b>人工复核路由</b>：若开启了人工复核（HUMAN_REVIEW_ENABLED），将流程暂停并路由到 HumanFeedback 节点</li>
 *   <li><b>步骤推进</b>：根据当前步骤编号（PLAN_CURRENT_STEP），确定下一步应该路由到哪个工具节点
 *      （SQL_GENERATE_NODE / PYTHON_GENERATE_NODE / REPORT_GENERATOR_NODE），并更新步骤计数器</li>
 * </ol>
 *
 * <p>在 StateGraph 的循环模式中，本节点会被多次调用——每完成一个步骤后，流程会回到本节点推进到下一步，
 * 直到所有步骤执行完毕后路由到 REPORT_GENERATOR_NODE（或 END）。
 *
 * <p>涉及的 AI 概念：
 * <ul>
 *   <li><b>Agent 循环（Agentic Loop）</b>：本节点是 Agent 循环的核心调度器，通过"执行→回调→推进"模式实现多步骤编排</li>
 *   <li><b>Human-in-the-Loop</b>：通过 interruptBefore 机制暂停流程，等待人工确认或修改计划</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see Plan 执行计划的数据结构
 * @see ExecutionStep 计划中的单个执行步骤
 */
@Slf4j
@Component
public class PlanExecutorNode implements NodeAction {

	/**
	 * 当前支持的工具节点类型集合。Plan 中每一步的 toolToUse 必须是以下之一：
	 * <ul>
	 *   <li>{@code SQL_GENERATE_NODE} — SQL 生成节点（Text-to-SQL）</li>
	 *   <li>{@code PYTHON_GENERATE_NODE} — Python 代码生成节点（深度数据分析）</li>
	 *   <li>{@code REPORT_GENERATOR_NODE} — 报告生成节点（HTML/Markdown 产出）</li>
	 * </ul>
	 */
	private static final Set<String> SUPPORTED_NODES = Set.of(SQL_GENERATE_NODE, PYTHON_GENERATE_NODE,
			REPORT_GENERATOR_NODE);

	/**
	 * 节点执行入口，由 StateGraph 框架在运行到此节点时自动调用。
	 *
	 * <p><b>输入状态读取：</b>
	 * <ul>
	 *   <li>{@code PLANNER_NODE_OUTPUT} — Planner 节点生成的执行计划 JSON</li>
	 *   <li>{@code HUMAN_REVIEW_ENABLED} — 是否开启人工复核</li>
	 *   <li>{@code PLAN_CURRENT_STEP} — 当前执行到第几步</li>
	 *   <li>{@code IS_ONLY_NL2SQL} — 是否为纯 NL2SQL 模式</li>
	 * </ul>
	 *
	 * <p><b>核心逻辑：</b>
	 * <ol>
	 *   <li>解析并验证 Plan JSON 结构</li>
	 *   <li>检查是否需要人工复核</li>
	 *   <li>推进步骤计数器，路由到对应的工具节点</li>
	 * </ol>
	 *
	 * <p><b>输出状态写入：</b>
	 * <ul>
	 *   <li>{@code PLAN_VALIDATION_STATUS} — 计划是否通过验证（false 会触发 Planner 重新生成）</li>
	 *   <li>{@code PLAN_VALIDATION_ERROR} — 验证失败的错误信息（传递给 Planner 用于修复）</li>
	 *   <li>{@code PLAN_NEXT_NODE} — 下一个应该执行的节点名称（用于条件路由）</li>
	 *   <li>{@code PLAN_CURRENT_STEP} — 更新后的步骤编号</li>
	 *   <li>{@code PLAN_REPAIR_COUNT} — 计划修复计数（防止无限重试）</li>
	 * </ul>
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {
		// TODO 待优化：校验逻辑应该在 Planner 生成计划之后立即执行，而不是放在执行节点中。
		// 当前设计导致每推进一个步骤都会重新校验整个计划，造成不必要的重复解析
		// 1. 解析 Plan JSON 并校验结构完整性
		Plan plan;
		try {
			plan = PlanProcessUtil.getPlan(state);
		}
		catch (Exception e) {
			log.error("Plan validation failed due to a parsing error.", e);
			return buildValidationResult(state, false,
					"Validation failed: The plan is not a valid JSON structure. Error: " + e.getMessage());
		}

		// 校验计划是否包含至少一个执行步骤
		if (!validateExecutionPlanStructure(plan)) {
			return buildValidationResult(state, false,
					"Validation failed: The generated plan is empty or has no execution steps.");
		}

		// 逐步骤校验：工具名称合法性、参数完整性、各工具类型的必填参数
		for (ExecutionStep step : plan.getExecutionPlan()) {
			String validationResult = validateExecutionStep(step);
			if (validationResult != null) {
				return buildValidationResult(state, false, validationResult);
			}
		}

		log.info("Plan validation successful.");
		// 2. 人工复核路由：如果开启，在执行前暂停流程，跳转到 human_feedback 节点等待人工确认
		Boolean humanReviewEnabled = state.value(HUMAN_REVIEW_ENABLED, false);
		if (Boolean.TRUE.equals(humanReviewEnabled)) {
			log.info("Human review enabled: routing to human_feedback node");
			return Map.of(PLAN_VALIDATION_STATUS, true, PLAN_NEXT_NODE, HUMAN_FEEDBACK_NODE);
		}

		// 3. 步骤推进：读取当前步骤编号，确定下一步要执行的节点
		int currentStep = PlanProcessUtil.getCurrentStepNumber(state);
		List<ExecutionStep> executionPlan = plan.getExecutionPlan();

		boolean isOnlyNl2Sql = state.value(IS_ONLY_NL2SQL, false);

		// 所有步骤已执行完毕：重置步骤计数器，路由到报告生成节点（或 END）
		if (currentStep > executionPlan.size()) {
			log.info("Plan completed, current step: {}, total steps: {}", currentStep, executionPlan.size());
			return Map.of(PLAN_CURRENT_STEP, 1, PLAN_NEXT_NODE, isOnlyNl2Sql ? StateGraph.END : REPORT_GENERATOR_NODE,
					PLAN_VALIDATION_STATUS, true);
		}

		// 取出当前步骤（步骤编号从 1 开始，列表索引从 0 开始，所以需要 -1）
		ExecutionStep executionStep = executionPlan.get(currentStep - 1);
		String toolToUse = executionStep.getToolToUse();

		return determineNextNode(toolToUse);
	}

	/**
	 * 根据工具名称确定下一步路由到哪个节点。
	 *
	 * <p>这是 StateGraph 条件边（conditional edge）的实现方式——
	 * 将 {@code PLAN_NEXT_NODE} 写入状态后，Graph 的路由逻辑会读取该值决定下一个节点。
	 *
	 * @param toolToUse 当前步骤要使用的工具名称（来自 Plan 的 toolToUse 字段）
	 * @return 包含 PLAN_NEXT_NODE 和 PLAN_VALIDATION_STATUS 的状态更新
	 */
	private Map<String, Object> determineNextNode(String toolToUse) {
		if (SUPPORTED_NODES.contains(toolToUse)) {
			log.info("Determined next execution node: {}", toolToUse);
			return Map.of(PLAN_NEXT_NODE, toolToUse, PLAN_VALIDATION_STATUS, true);
		}
		else if (HUMAN_FEEDBACK_NODE.equals(toolToUse)) {
			log.info("Determined next execution node: {}", toolToUse);
			return Map.of(PLAN_NEXT_NODE, toolToUse, PLAN_VALIDATION_STATUS, true);
		}
		else {
			// 理论上不会走到这里，因为上面的 validateExecutionStep 已经校验了工具名称
			return Map.of(PLAN_VALIDATION_STATUS, false, PLAN_VALIDATION_ERROR, "Unsupported node type: " + toolToUse);
		}
	}

	/**
	 * 校验计划结构：非空、包含至少一个执行步骤。
	 */
	private boolean validateExecutionPlanStructure(Plan plan) {
		return plan != null && plan.getExecutionPlan() != null && !plan.getExecutionPlan().isEmpty();
	}

	/**
	 * 校验单个执行步骤的合法性。
	 *
	 * <p>校验维度：
	 * <ol>
	 *   <li>工具名称必须属于 {@link #SUPPORTED_NODES}</li>
	 *   <li>工具参数（ToolParameters）不能为 null</li>
	 *   <li>针对不同工具类型校验必填参数：
	 *     <ul>
	 *       <li>SQL_GENERATE_NODE — 需要 instruction（SQL 生成指令描述）</li>
	 *       <li>PYTHON_GENERATE_NODE — 需要 instruction（Python 代码生成指令）</li>
	 *       <li>REPORT_GENERATOR_NODE — 需要 summaryAndRecommendations（总结与建议）</li>
	 *     </ul>
	 *   </li>
	 * </ol>
	 *
	 * @return 错误信息字符串，校验通过返回 null
	 */
	private String validateExecutionStep(ExecutionStep step) {
		// 校验工具名称：LLM 生成的 toolToUse 可能是不受支持的值，需要严格校验
		if (step.getToolToUse() == null || !SUPPORTED_NODES.contains(step.getToolToUse())) {
			return "Validation failed: Plan contains an invalid tool name: '" + step.getToolToUse() + "' in step "
					+ step.getStep();
		}

		// 校验工具参数存在性
		if (step.getToolParameters() == null) {
			return "Validation failed: Tool parameters are missing for step " + step.getStep();
		}

		// 针对不同工具类型校验各自的必填参数
		switch (step.getToolToUse()) {
			case SQL_GENERATE_NODE:
				if (!StringUtils.hasText(step.getToolParameters().getInstruction())) {
					return "Validation failed: SQL generation node is missing description in step " + step.getStep();
				}
				break;

			case PYTHON_GENERATE_NODE:
				if (!StringUtils.hasText(step.getToolParameters().getInstruction())) {
					return "Validation failed: Python generation node is missing instruction in step " + step.getStep();
				}
				break;

			case REPORT_GENERATOR_NODE:
				if (!StringUtils.hasText(step.getToolParameters().getSummaryAndRecommendations())) {
					return "Validation failed: Report generation node is missing summary_and_recommendations in step "
							+ step.getStep();
				}
				break;

			default:
				break;
		}

		return null; // Validation passed
	}

	/**
	 * 构建验证结果状态更新。
	 *
	 * <p>验证失败时会递增 {@code PLAN_REPAIR_COUNT}，这是防止 Agent 无限循环的安全机制——
	 * 当修复次数超过阈值时，工作流会终止并返回错误。
	 *
	 * @param state 全局状态，用于读取当前修复计数
	 * @param isValid 计划是否通过验证
	 * @param errorMessage 验证失败的错误信息（传递给 Planner 节点用于修复计划）
	 * @return 状态更新 Map
	 */
	private Map<String, Object> buildValidationResult(OverAllState state, boolean isValid, String errorMessage) {
		if (isValid) {
			return Map.of(PLAN_VALIDATION_STATUS, true);
		}
		else {
			// 验证失败时递增修复计数，StateGraph 外层逻辑会检查该值是否超过最大重试次数
			int repairCount = StateUtil.getObjectValue(state, PLAN_REPAIR_COUNT, Integer.class, 0);
			return Map.of(PLAN_VALIDATION_STATUS, false, PLAN_VALIDATION_ERROR, errorMessage, PLAN_REPAIR_COUNT,
					repairCount + 1);
		}
	}

}
