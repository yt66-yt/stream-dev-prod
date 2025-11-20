// 新增文件：PostgreSQLUtils.java
package com.stream.realtime.lululemon.uster;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgreSQLUtils {

    private static final String JDBC_URL = "jdbc:postgresql://192.168.200.30:5432/spider_db";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "pgsql"; // 请替换为实际密码

    public static JdbcConnectionOptions getJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .build();
    }
}