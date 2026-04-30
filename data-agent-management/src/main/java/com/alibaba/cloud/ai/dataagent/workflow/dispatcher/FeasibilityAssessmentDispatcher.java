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
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.FEASIBILITY_ASSESSMENT_NODE_OUTPUT;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.PLANNER_NODE;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 可行性评估节点（FeasibilityAssessmentNode）的路由分发器。
 * <p>
 * 可行性评估节点由 LLM 对用户需求进行分类判断，输出格式遵循预设模板，例如：
 * <pre>
 *   【需求类型】：《数据分析》
 *   【语种类型】：《中文》
 *   【需求内容】：查询所有”核心用户”的数量
 * </pre>
 * 本分发器解析 LLM 输出中的”需求类型”字段：
 * <ul>
 *   <li>需求类型为”数据分析” → 路由到 PlannerNode，开始制定执行计划</li>
 *   <li>其他类型（如”闲聊”、”非数据需求”等）→ 直接结束流程</li>
 * </ul>
 *
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class FeasibilityAssessmentDispatcher implements EdgeAction {

	/**
	 * 根据可行性评估的需求类型分类决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return PLANNER_NODE（计划生成节点）或 END
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		// 获取 LLM 输出的结构化评估结果，格式与 feasibility-assessment.txt 提示词模板对应
		String value = state.value(FEASIBILITY_ASSESSMENT_NODE_OUTPUT, END);

		// 通过字符串匹配解析需求类型（LLM输出格式由Prompt模板约束）
		if (value != null && value.contains(“【需求类型】：《数据分析》”)) {
			log.info(“[FeasibilityAssessmentNodeDispatcher]需求类型为数据分析，进入PlannerNode节点”);
			return PLANNER_NODE;
		}
		else {
			log.info(“[FeasibilityAssessmentNodeDispatcher]需求类型非数据分析，返回END节点”);
			return END;
		}
	}

}
