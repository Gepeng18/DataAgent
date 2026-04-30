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
package com.alibaba.cloud.ai.dataagent.connector;

import com.alibaba.cloud.ai.dataagent.bo.schema.ResultSetBO;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet 到业务对象（ResultSetBO）的转换器。
 * <p>
 * 在 DataAgent 中，LLM 生成的 SQL 执行后返回的是 JDBC 原生的 ResultSet 对象，
 * 需要转换成结构化的 {@link ResultSetBO}（包含列名列表和每行数据的键值对列表）
 * 才能供后续的报告生成、LLM 分析等环节使用。
 * <p>
 * 本类还负责列名的"清洗"——移除不同数据库方言使用的引号符号（MySQL 的反引号、
 * ANSI 标准的双引号），统一列名格式，避免下游处理时因引号导致键匹配失败。
 *
 * @see ResultSetBO ResultSetBO — SQL 查询结果的结构化表示
 * @see SqlExecutor SqlExecutor — 调用本类的 SQL 执行器
 */
public class ResultSetBuilder {

	/**
	 * 将 JDBC ResultSet 转换为 ResultSetBO 业务对象。
	 * <p>
	 * 转换流程：
	 * <ol>
	 *   <li>从 ResultSetMetaData 提取列名作为表头</li>
	 *   <li>逐行遍历 ResultSet，将每行数据构建为 Map&lt;列名, 值&gt;</li>
	 *   <li>清洗列名和数据中的引号符号</li>
	 * </ol>
	 * <p>
	 * 注意：行数限制采用双重保障——JDBC 层面通过 Statement.setMaxRows() 限制，
	 * 这里额外通过手动计数器确保不超过 {@link SqlExecutor#RESULT_SET_LIMIT}。
	 *
	 * @param rs     JDBC 查询结果集
	 * @param schema 当前 Schema 名称（传递给 ResultSetBO 用于上下文标识）
	 * @return ResultSetBO 包含清洗后的列名列表和数据行列表
	 * @throws SQLException JDBC 结果集读取异常
	 */
	public static ResultSetBO buildFrom(ResultSet rs, String schema) throws SQLException {
		ResultSetMetaData data = rs.getMetaData();
		int columnsCount = data.getColumnCount();
		ResultSetBO resultSetBO = new ResultSetBO();
		String[] rowHead = new String[columnsCount];

		// JDBC 列索引从 1 开始（而非 0），这是 JDBC 规范的约定
		for (int i = 1; i <= columnsCount; i++) {
			rowHead[i - 1] = data.getColumnLabel(i);
		}

		List<Map<String, String>> resultSetData = Lists.newArrayList();
		int count = 0;

		// 双重保险：JDBC setMaxRows + 手动计数器，确保不超过行数上限
		while (rs.next() && count < SqlExecutor.RESULT_SET_LIMIT) {
			Map<String, String> kv = new HashMap<>();
			for (String h : rowHead) {
				// 将 null 值统一转为空字符串，避免下游 NPE
				kv.put(h, rs.getString(h) == null ? "" : rs.getString(h));
			}
			resultSetData.add(kv);
			count++;
		}

		// 清洗列名：移除数据库方言特有的引号符号（MySQL反引号`、ANSI双引号"）
		List<String> cleanedHead = cleanColumnNames(Arrays.asList(rowHead));
		List<Map<String, String>> cleanedData = cleanResultSet(resultSetData);

		resultSetBO.setColumn(cleanedHead);
		resultSetBO.setData(cleanedData);

		return resultSetBO;
	}

	/**
	 * 清洗列名，移除不同数据库方言使用的引号包裹符号。
	 * <p>
	 * 例如：MySQL 中 {@code `user_name`} → {@code user_name}，
	 * Oracle/ANSI 中 {@code "USER_NAME"} → {@code USER_NAME}。
	 * 统一列名格式后，后续通过列名获取数据时不会因引号导致匹配失败。
	 */
	private static List<String> cleanColumnNames(List<String> columnNames) {
		return columnNames.stream().map(name -> StringUtils.remove(StringUtils.remove(name, "`"), "\"")).toList();
	}

	/**
	 * 清洗结果数据中每行 Map 的键，移除键名中的引号符号。
	 * <p>
	 * 与 {@link #cleanColumnNames(List)} 配合，确保列名清洗后数据 Map 的键
	 * 也能与之对应，否则会出现列名与数据键不匹配的问题。
	 */
	private static List<Map<String, String>> cleanResultSet(List<Map<String, String>> data) {
		return data.stream().map(row -> {
			Map<String, String> cleanedRow = new HashMap<>();
			row.forEach((k, v) -> {
				String cleanedKey = StringUtils.remove(k, "`");
				cleanedKey = StringUtils.remove(cleanedKey, "\"");
				cleanedRow.put(cleanedKey, v);
			});
			return cleanedRow;
		}).toList();
	}

}
