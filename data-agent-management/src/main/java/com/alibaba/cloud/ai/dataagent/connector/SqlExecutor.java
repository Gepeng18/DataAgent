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
import com.alibaba.cloud.ai.dataagent.enums.DatabaseDialectEnum;
import com.alibaba.cloud.ai.dataagent.util.ResultSetConvertUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * SQL 执行器——封装了 JDBC 执行 SQL 的核心逻辑，是 DataAgent 与数据库交互的底层组件。
 * <p>
 * 本类提供两类执行结果格式：
 * <ul>
 *   <li>{@link ResultSetBO}（结构化对象）：包含列名列表 + 每行的键值对数据，供后续报告生成和 LLM 分析使用</li>
 *   <li>String[][]（二维字符串数组）：轻量级格式，适用于 LLM Prompt 中直接展示查询结果</li>
 * </ul>
 * <p>
 * 关键设计：
 * <ul>
 *   <li>所有方法均为 static，通过传入 Connection 实现无状态执行</li>
 *   <li>内置安全限制（行数上限、查询超时），防止 LLM 生成的 SQL 导致资源耗尽</li>
 *   <li>支持多数据库方言（MySQL/PostgreSQL/H2/Oracle）的 Schema 切换</li>
 * </ul>
 *
 * @see ResultSetBuilder ResultSetBuilder — 将 JDBC ResultSet 转换为 ResultSetBO
 * @see ResultSetBO ResultSetBO — SQL 查询结果的结构化表示
 */
public class SqlExecutor {

	/**
	 * 结果集最大行数限制，防止 LLM 生成的查询（如无 WHERE 的全表扫描）返回海量数据导致 OOM。
	 * 此限制通过 JDBC Statement.setMaxRows() 实现，由数据库驱动在传输层面截断。
	 */
	public static final Integer RESULT_SET_LIMIT = 1000;

	/**
	 * SQL 查询超时时间（秒），防止 LLM 生成的低效 SQL 长时间占用数据库连接。
	 * 超时后 JDBC 驱动会抛出 SQLTimeoutException。
	 */
	public static final Integer STATEMENT_TIMEOUT = 30;

	/**
	 * 执行 SQL 并返回结构化结果对象（包含列名 + 键值对数据）。
	 * <p>
	 * 这是 SqlExecuteNode 调用的主要方法。执行流程：
	 * <ol>
	 *   <li>创建 Statement 并设置安全限制（行数、超时）</li>
	 *   <li>根据数据库方言执行 Schema 切换（不同数据库的切换语法不同）</li>
	 *   <li>执行 SQL 查询并通过 ResultSetBuilder 转换为结构化对象</li>
	 * </ol>
	 *
	 * @param connection 数据库连接（由数据源连接池管理）
	 * @param schema     目标 Schema/数据库名称，用于多租户场景下的 Schema 隔离
	 * @param sql        LLM 生成的 SQL 语句
	 * @return ResultSetBO 包含列名和数据的结构化结果
	 * @throws SQLException SQL 执行异常
	 */
	public static ResultSetBO executeSqlAndReturnObject(Connection connection, String schema, String sql)
			throws SQLException {
		try (Statement statement = connection.createStatement()) {
			// 设置安全限制：最大返回行数和查询超时
			statement.setMaxRows(RESULT_SET_LIMIT);
			statement.setQueryTimeout(STATEMENT_TIMEOUT);

			// 通过 DatabaseMetaData 检测数据库方言，不同数据库的 Schema 切换语法差异较大
			DatabaseMetaData metaData = connection.getMetaData();
			String dialect = metaData.getDatabaseProductName();

			if (dialect.equals(DatabaseDialectEnum.POSTGRESQL.code)) {
				// PostgreSQL 使用 search_path 控制 Schema 搜索路径
				if (StringUtils.isNotEmpty(schema)) {
					statement.execute("set search_path = '" + schema + "';");
				}
			}
			else if (dialect.equals(DatabaseDialectEnum.H2.code)) {
				// H2 内存数据库，开发环境使用
				if (StringUtils.isNotEmpty(schema)) {
					statement.execute("use " + schema + ";");
				}
			}
			else if (dialect.equals(DatabaseDialectEnum.ORACLE.code)) {
				// Oracle 通过修改当前会话 Schema 实现
				if (StringUtils.isNotEmpty(schema)) {
					statement.execute("ALTER SESSION SET CURRENT_SCHEMA = " + schema);
				}
			}

			try (ResultSet rs = statement.executeQuery(sql)) {
				// 将 JDBC ResultSet 转换为业务对象
				return ResultSetBuilder.buildFrom(rs, schema);
			}
		}
	}

	/**
	 * 执行 SQL 并返回二维字符串数组格式的结果（不含 Schema 切换）。
	 *
	 * @param connection 数据库连接
	 * @param sql        SQL 语句
	 * @return String[][] 二维数组，第一维是行，第二维是列值
	 * @throws SQLException SQL 执行异常
	 */
	public static String[][] executeSqlAndReturnArr(Connection connection, String sql) throws SQLException {
		List<String[]> list = executeQuery(connection, sql);
		return list.toArray(new String[0][]);
	}

	/**
	 * 执行 SQL 并返回二维字符串数组格式的结果（带 Schema 切换）。
	 * <p>
	 * 与无 Schema 版本不同，此方法在执行前会切换到指定的 Schema，
	 * 执行完成后对 MySQL 会恢复原始数据库连接。
	 *
	 * @param connection       数据库连接
	 * @param databaseOrSchema 目标数据库名或 Schema 名
	 * @param sql              SQL 语句
	 * @return String[][] 二维数组结果
	 * @throws SQLException SQL 执行异常
	 */
	public static String[][] executeSqlAndReturnArr(Connection connection, String databaseOrSchema, String sql)
			throws SQLException {
		List<String[]> list = executeQuery(connection, databaseOrSchema, sql);
		return list.toArray(new String[0][]);
	}

	/**
	 * 无 Schema 切换的 SQL 查询，返回 List&lt;String[]&gt; 格式。
	 */
	private static List<String[]> executeQuery(Connection connection, String sql) throws SQLException {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {

			return ResultSetConvertUtil.convert(rs);
		}
	}

	/**
	 * 带 Schema 切换的 SQL 查询，返回 List&lt;String[]&gt; 格式。
	 * <p>
	 * 执行流程：
	 * <ol>
	 *   <li>记录当前连接的 Catalog（MySQL 中即数据库名）</li>
	 *   <li>根据数据库方言执行对应的 Schema 切换语句</li>
	 *   <li>执行 SQL 查询并转换结果</li>
	 *   <li>对 MySQL 恢复原始数据库连接（因为 MySQL 的 USE 会影响连接状态）</li>
	 * </ol>
	 * <p>
	 * 注意：只对 MySQL 做了恢复操作，PostgreSQL 和 Oracle 的 Schema 切换
	 * 是会话级的临时设置，不影响连接池中连接的后续复用。
	 */
	private static List<String[]> executeQuery(Connection connection, String databaseOrSchema, String sql)
			throws SQLException {
		// 记录原始数据库，用于 MySQL 场景下的恢复
		String originalDb = connection.getCatalog();
		DatabaseMetaData metaData = connection.getMetaData();
		String dialect = metaData.getDatabaseProductName();

		try (Statement statement = connection.createStatement()) {

			// 根据数据库方言执行不同的 Schema 切换语句
			if (dialect.equals(DatabaseDialectEnum.MYSQL.code)) {
				// MySQL 使用反引号包裹数据库名，防止保留字冲突
				if (StringUtils.isNotEmpty(databaseOrSchema)) {
					statement.execute("use `" + databaseOrSchema + "`;");
				}
			}
			else if (dialect.equals(DatabaseDialectEnum.POSTGRESQL.code)) {
				if (StringUtils.isNotEmpty(databaseOrSchema)) {
					statement.execute("set search_path = '" + databaseOrSchema + "';");
				}
			}
			else if (dialect.equals(DatabaseDialectEnum.ORACLE.code)) {
				if (StringUtils.isNotEmpty(databaseOrSchema)) {
					statement.execute("ALTER SESSION SET CURRENT_SCHEMA = " + databaseOrSchema);
				}
			}

			ResultSet rs = statement.executeQuery(sql);

			List<String[]> result = ResultSetConvertUtil.convert(rs);

			// MySQL 需要恢复原始数据库：因为 USE 语句是持久性的，会影响连接池中该连接的后续使用
			if (StringUtils.isNotEmpty(databaseOrSchema) && dialect.equals(DatabaseDialectEnum.MYSQL.code)) {
				statement.execute("use `" + originalDb + "`;");
			}

			return result;
		}
	}

}
