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
package com.alibaba.cloud.ai.dataagent.dto.datasource;

/**
 * SQL 重试原因的数据载体（Java Record）。
 * <p>
 * 在 DataAgent 的 Text-to-SQL 流水线中，SQL 可能因两种不同的原因需要重新生成：
 * <ul>
 *   <li><b>语义一致性校验失败</b>（semanticFail）— LLM 生成的 SQL 虽然语法正确，
 *       但语义上没有正确表达用户的查询意图，需要回到 SqlGenerateNode 重新生成</li>
 *   <li><b>SQL 执行失败</b>（sqlExecuteFail）— SQL 在数据库中执行时报错
 *      （语法错误、权限不足、表不存在等），需要携带错误信息回到 SqlGenerateNode 修正</li>
 * </ul>
 * <p>
 * 使用工厂方法（{@link #semantic}、{@link #sqlExecute}、{@link #empty}）创建实例，
 * 确保两种失败标记互斥——同一次重试只有一种原因。{@link #empty()} 表示无需重试。
 * <p>
 * 该 DTO 在工作流状态中通过 {@code SQL_REGENERATE_REASON} 键传递，
 * 由 {@code SQLExecutorDispatcher} 和 {@code SemanticConsistenceDispatcher} 读取。
 *
 * @param reason         人类可读的重试原因描述（通常是 LLM 给出的解释或数据库返回的错误信息）
 * @param semanticFail   是否因语义一致性校验失败而重试
 * @param sqlExecuteFail 是否因 SQL 执行错误而重试
 */
public record SqlRetryDto(String reason, boolean semanticFail, boolean sqlExecuteFail) {

	/**
	 * 创建"语义一致性校验失败"类型的重试 DTO。
	 *
	 * @param reason 校验失败的原因描述
	 * @return semanticFail=true, sqlExecuteFail=false 的 DTO 实例
	 */
	public static SqlRetryDto semantic(String reason) {
		return new SqlRetryDto(reason, true, false);
	}

	/**
	 * 创建"SQL 执行失败"类型的重试 DTO。
	 *
	 * @param reason 数据库返回的错误信息
	 * @return semanticFail=false, sqlExecuteFail=true 的 DTO 实例
	 */
	public static SqlRetryDto sqlExecute(String reason) {
		return new SqlRetryDto(reason, false, true);
	}

	/**
	 * 创建空的重试 DTO，表示 SQL 无需重试（执行成功）。
	 *
	 * @return semanticFail=false, sqlExecuteFail=false 的 DTO 实例
	 */
	public static SqlRetryDto empty() {
		return new SqlRetryDto("", false, false);
	}

}
