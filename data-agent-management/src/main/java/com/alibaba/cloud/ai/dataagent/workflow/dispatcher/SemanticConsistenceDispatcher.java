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

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 语义一致性校验节点（SemanticConsistencyNode）的路由分发器。
 * <p>
 * 语义一致性校验是 Text-to-SQL 流水线中的质量控制环节：由另一个 LLM 调用
 * 对生成的 SQL 进行"双重检查"，验证 SQL 是否在语义上正确反映了用户的查询意图
 * （而非仅仅语法正确）。这有助于捕获 LLM 生成 SQL 中的"幻觉"问题，例如
 * 查询了错误的表、遗漏了过滤条件等。
 * <p>
 * 本分发器的路由逻辑：
 * <ul>
 *   <li>校验通过 → 路由到 SqlExecuteNode 实际执行 SQL</li>
 *   <li>校验未通过 → 路由回 SqlGenerateNode 重新生成 SQL（形成 SQL 优化的反馈循环）</li>
 * </ul>
 *
 * @author zhangshenghang
 * @see com.alibaba.cloud.ai.graph.action.EdgeAction
 */
@Slf4j
public class SemanticConsistenceDispatcher implements EdgeAction {

	/**
	 * 根据语义一致性校验结果决定下一个节点。
	 *
	 * @param state 工作流全局状态对象
	 * @return SQL_EXECUTE_NODE（执行SQL）或 SQL_GENERATE_NODE（重新生成SQL）
	 */
	@Override
	public String apply(OverAllState state) {
		// SemanticConsistencyNode 的输出是一个布尔值，表示 SQL 是否通过了语义校验
		Boolean validate = (Boolean) state.value(SEMANTIC_CONSISTENCY_NODE_OUTPUT).orElse(false);
		log.info("语义一致性校验结果: {}，跳转节点配置", validate);
		if (validate) {
			log.info("语义一致性校验通过，跳转到SQL运行节点。");
			return SQL_EXECUTE_NODE;
		}
		else {
			// 校验未通过，回到SQL生成节点重新生成，形成"生成-校验"的迭代循环
			log.info("语义一致性校验未通过，跳转到SQL生成节点。");
			return SQL_GENERATE_NODE;
		}
	}

}
