// UserInfoPostgresSource.java
package com.stream.realtime.lululemon.uster.ljtable;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.configuration.Configuration;
import com.alibaba.fastjson2.JSONObject;

import java.sql.*;

/**
 * 使用RichSourceFunction的PostgreSQL源，兼容所有Flink版本
 */
public class UserInfoPostgresSource extends RichSourceFunction<JSONObject> {

    private volatile boolean isRunning = true;
    private Connection connection;
    private PreparedStatement preparedStatement;

    private final String dbUrl;
    private final String username;
    private final String password;
    private final long queryIntervalMs;

    public UserInfoPostgresSource(String dbUrl, String username, String password, long queryIntervalMs) {
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;
        this.queryIntervalMs = queryIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 注册驱动
        Class.forName("org.postgresql.Driver");
        // 创建连接
        connection = DriverManager.getConnection(dbUrl, username, password);
        preparedStatement = connection.prepareStatement(
                "SELECT user_id, uname, phone_num, birthday, gender, address, ts " +
                        "FROM spider_db_public_user_info_base"
        );

        System.out.println("✅ PostgreSQL连接建立成功: " + dbUrl);
    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
        while (isRunning) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                int recordCount = 0;

                while (resultSet.next() && isRunning) {
                    JSONObject userInfo = new JSONObject();
                    userInfo.put("user_id", resultSet.getString("user_id"));
                    userInfo.put("uname", resultSet.getString("uname"));
                    userInfo.put("phone_num", resultSet.getString("phone_num"));
                    userInfo.put("birthday", resultSet.getString("birthday"));
                    userInfo.put("gender", resultSet.getString("gender"));
                    userInfo.put("address", resultSet.getString("address"));
                    userInfo.put("ts", resultSet.getString("ts"));
                    userInfo.put("source_type", "user_info_base");

                    // 发送数据
                    ctx.collect(userInfo);
                    recordCount++;
                }

                System.out.println("✅ 从PostgreSQL读取 " + recordCount + " 条用户记录");

                // 间隔一段时间后重新查询（用于数据更新）
                if (isRunning) {
                    Thread.sleep(queryIntervalMs);
                }

            } catch (Exception e) {
                System.err.println("❌ PostgreSQL查询错误: " + e.getMessage());
                e.printStackTrace();

                // 发生错误时等待一段时间后重试
                if (isRunning) {
                    Thread.sleep(60000); // 等待1分钟后重试
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

        // 清理资源
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            System.out.println("✅ PostgreSQL连接已关闭");
        } catch (SQLException e) {
            System.err.println("❌ 关闭PostgreSQL连接时出错: " + e.getMessage());
        }
    }

    // 静态工厂方法
    public static UserInfoPostgresSource createUserInfoSource() {
        String dbUrl = "jdbc:postgresql://192.168.200.30:5432/spider_db";
        String username = "postgres";
        String password = "pgsql"; // 请替换为实际密码
        long queryIntervalMs = 300000; // 5分钟间隔

        return new UserInfoPostgresSource(dbUrl, username, password, queryIntervalMs);
    }

    // 便捷方法，返回SourceFunction
    public static RichSourceFunction<JSONObject> createSourceFunction() {
        return createUserInfoSource();
    }
}