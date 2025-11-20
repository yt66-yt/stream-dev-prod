package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.sql.*;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 异步查询PostgreSQL用户信息
 */
public class PostgreSQLAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {

    private transient Connection connection;
    private transient PreparedStatement preparedStatement;
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 手动创建线程池 - 更精细的控制
        executorService = new ThreadPoolExecutor(
                // 核心线程数
                5,
                // 最大线程数
                20,
                // 空闲线程存活时间
                60L,
                // 时间单位
                TimeUnit.SECONDS,
                // 工作队列 - 使用有界队列避免内存溢出
                new LinkedBlockingQueue<>(1000),
                // 线程工厂 - 自定义线程名称便于监控
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "postgresql-async-" + threadNumber.getAndIncrement());
                        thread.setDaemon(true); // 设置为守护线程
                        return thread;
                    }
                },
                // 拒绝策略 - 调用者运行，避免数据丢失
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // 初始化数据库连接
        try {
            Class.forName("org.postgresql.Driver");
            // 使用您提供的IP地址和密码
            String url = "jdbc:postgresql://192.168.200.30:5432/spider_db";
            String username = "postgres";
            String password = "pgsql";

            connection = DriverManager.getConnection(url, username, password);
            String sql = "SELECT uname, phone_num, birthday, gender, address, ts " +
                    "FROM spider_db_public_user_info_base WHERE user_id = ?";
            preparedStatement = connection.prepareStatement(sql);

            System.out.println("✅ PostgreSQL异步函数初始化成功，线程池已创建");
        } catch (Exception e) {
            System.err.println("❌ PostgreSQL异步函数初始化失败: " + e.getMessage());
            // 关闭线程池
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }

    @Override
    public void close() throws Exception {
        // 优雅关闭线程池
        if (executorService != null) {
            executorService.shutdown();
            try {
                // 等待现有任务完成
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    // 强制关闭
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

        super.close();
    }

    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        // 如果数据库连接失败，直接返回原始数据
        if (connection == null || connection.isClosed()) {
            System.err.println("⚠️ 数据库连接不可用，跳过用户信息查询");
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        String userId = input.getString("user_id");

        CompletableFuture.supplyAsync(() -> {
            try {
                return queryUserInfo(userId);
            } catch (SQLException e) {
                System.err.println("❌ 查询用户信息失败: " + e.getMessage());
                return null;
            }
        }, executorService).thenAccept(userInfo -> {
            // 合并数据
            JSONObject result = new JSONObject();
            result.putAll(input);
            if (userInfo != null) {
                result.putAll(userInfo);
                System.out.println("✅ 成功关联用户信息: " + userId);
            } else {
                System.out.println("⚠️ 未找到用户信息: " + userId);
            }
            resultFuture.complete(Collections.singleton(result));
        }).exceptionally(throwable -> {
            System.err.println("❌ 异步查询失败: " + throwable.getMessage());
            // 即使查询失败，也返回原始数据
            resultFuture.complete(Collections.singleton(input));
            return null;
        });
    }

    private JSONObject queryUserInfo(String userId) throws SQLException {
        if (userId == null || userId.trim().isEmpty()) {
            return null;
        }

        preparedStatement.setString(1, userId);
        ResultSet resultSet = preparedStatement.executeQuery();

        JSONObject userInfo = new JSONObject();
        if (resultSet.next()) {
            userInfo.put("uname", resultSet.getString("uname"));
            userInfo.put("phone_num", resultSet.getString("phone_num"));
            userInfo.put("birthday", resultSet.getString("birthday"));
            userInfo.put("gender", resultSet.getString("gender"));
            userInfo.put("address", resultSet.getString("address"));
            userInfo.put("user_info_ts", resultSet.getString("ts"));
        }

        resultSet.close();
        return userInfo;
    }

    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        System.err.println("⏰ 用户信息查询超时: " + input.getString("user_id"));
        // 超时后返回原始数据
        resultFuture.complete(Collections.singleton(input));
    }
}