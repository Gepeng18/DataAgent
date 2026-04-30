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

import com.alibaba.cloud.ai.dataagent.dto.prompt.IntentRecognitionOutputDTO;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.EdgeAction;
import com.alibaba.cloud.ai.dataagent.util.StateUtil;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.EVIDENCE_RECALL_NODE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.INTENT_RECOGNITION_NODE_OUTPUT;
import static com.alibaba.cloud.ai.graph.StateGraph.END;

/**
 * 意图识别节点的路由分发器。
 * <p>
 * 在 StateGraph 工作流中，Dispatcher（分发器）充当"条件边"的角色——它根据上游节点的输出
 * 决定下一个要执行的节点。本分发器读取 IntentRecognitionNode 的分类结果进行路由：
 * <ul>
 *   <li>如果 LLM 判定用户输入为"闲聊或无关指令"，则直接结束流程（返回 END）</li>
 *   <li>否则，认为用户可能提出了数据分析需求，路由到 EvidenceRecallNode（证据召回/RAG检索）继续处理</li>
 * </ul>
 * <p>
 * 这是整个工作流的第一个路由点，决定了用户的输入是否值得进入完整的数据分析流水线。
 *
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction EdgeAction — StateGraph 中条件边的统一接口
 */
@Slf4j
public class IntentRecognitionDispatcher implements EdgeAction {

	/**
	 * 根据意图识别结果决定工作流的下一个节点。
	 *
	 * @param state 工作流全局状态对象，所有节点通过它共享数据（类似有限状态机的状态）
	 * @return 下一个节点的名称常量（如 EVIDENCE_RECALL_NODE 或 END）
	 */
	@Override
	public String apply(OverAllState state) throws Exception {
		// 从全局状态中取出意图识别节点的输出DTO
		IntentRecognitionOutputDTO intentResult = StateUtil.getObjectValue(state, INTENT_RECOGNITION_NODE_OUTPUT,
				IntentRecognitionOutputDTO.class);

		// 防御性检查：LLM返回结果可能为null或空，此时无法路由，直接结束
		if (intentResult == null || intentResult.getClassification() == null
				|| intentResult.getClassification().trim().isEmpty()) {
			log.warn("Intent recognition result is null or empty, defaulting to END");
			return END;
		}

		String classification = intentResult.getClassification();

		// LLM Prompt 中预定义的分类标签，"《闲聊或无关指令》"表示用户的输入与数据分析无关
		if ("《闲聊或无关指令》".equals(classification)) {
			log.warn("Intent classified as chat or irrelevant, ending conversation");
			return END;
		}
		else {
			// 分类为数据分析相关，进入证据召回节点（基于向量数据库的RAG检索）
			log.info("Intent classified as potential data analysis request, proceeding to evidence recall");
			return EVIDENCE_RECALL_NODE;
		}
	}

}
