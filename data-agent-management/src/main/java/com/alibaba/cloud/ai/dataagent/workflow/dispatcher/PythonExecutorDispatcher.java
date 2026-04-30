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
import com.alibaba.cloud.ai.dataagent.properties.CodeExecutorProperties;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * Python 执行节点（PythonExecuteNode）的路由分发器。
 * <p>
 * 在 DataAgent 的数据分析流水线中，部分复杂需求需要通过 Python 代码执行来补充 SQL 无法
 * 完成的分析任务（例如复杂的统计计算、数据可视化等）。本分发器管理 Python 代码执行后
 * 的路由逻辑，包含三种场景：
 * <ol>
 *   <li><b>降级模式</b>：当代码执行环境不可用（如 Docker 未启动）时，设置 fallback 标记，
 *       跳过重试直接进入分析节点（PythonAnalyzeNode 会基于 LLM 模拟分析结果）</li>
 *   <li><b>执行失败</b>：在重试次数上限内路由回 PythonGenerateNode，让 LLM 根据错误信息
 *       修正代码后重新生成；超过最大重试次数则终止流程</li>
 *   <li><b>执行成功</b>：路由到 PythonAnalyzeNode，分析执行结果并生成报告数据</li>
 * </ol>
 *
 * @author vlsmb
 * @since 2025/7/29
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class PythonExecutorDispatcher implements EdgeAction {

	private final CodeExecutorProperties codeExecutorProperties;

	public PythonExecutorDispatcher(CodeExecutorProperties codeExecutorProperties) {
		this.codeExecutorProperties = codeExecutorProperties;
	}

	/**
	 * 根据 Python 代码执行结果决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return PYTHON_ANALYZE_NODE（分析执行结果）、PYTHON_GENERATE_NODE（重新生成代码）或 END
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		// 检查是否处于降级模式——执行环境不可用时，LLM 会模拟代码执行结果
		boolean isFallbackMode = StateUtil.getObjectValue(state, PYTHON_FALLBACK_MODE, Boolean.class, false);
		if (isFallbackMode) {
			log.warn("Python执行进入降级模式，跳过重试直接进入分析节点");
			return PYTHON_ANALYZE_NODE;
		}

		// 检查 Python 代码是否执行成功
		boolean isSuccess = StateUtil.getObjectValue(state, PYTHON_IS_SUCCESS, Boolean.class, false);
		if (!isSuccess) {
			String message = StateUtil.getStringValue(state, PYTHON_EXECUTE_NODE_OUTPUT);
			log.error("Python Executor Node Error: {}", message);
			int tries = StateUtil.getObjectValue(state, PYTHON_TRIES_COUNT, Integer.class, 0);
			if (tries >= codeExecutorProperties.getPythonMaxTriesCount()) {
				// 超过最大重试次数，终止流程
				log.error("Python执行失败且已超过最大重试次数（已尝试次数：{}），流程终止", tries);
				return END;
			}
			else {
				// 未超上限，路由回代码生成节点，LLM 会参考错误信息修正代码
				return PYTHON_GENERATE_NODE;
			}
		}
		// 执行成功，进入结果分析节点
		return PYTHON_ANALYZE_NODE;
	}

}
