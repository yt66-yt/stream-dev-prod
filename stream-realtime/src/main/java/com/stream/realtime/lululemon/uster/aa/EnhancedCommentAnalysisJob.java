package com.stream.realtime.lululemon.uster.aa;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import com.stream.realtime.lululemon.uster.MapComment;
import com.stream.realtime.lululemon.uster.MapUserImage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

/**
 * @author A
 */
public class EnhancedCommentAnalysisJob {

    private static final String KAFKA_TOPIC_LOGS = "realtime_v3_logs";
    private static final String KAFKA_TOPIC_COMMENT = "realtime_v3_order_comment";
    private static final String KAFKA_BOOTSTRAP = "172.24.158.53:9092";
    private static final String CONSUMER_GROUP_LOGS = "DBusUserProtraitLabel";
    private static final String CONSUMER_GROUP_COMMENT = "order_comment_processed";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置固定的网络内存配置以解决 IllegalConfigurationException
        // 设置固定的网络内存配置以解决 IllegalConfigurationException
        Configuration conf = new Configuration();
        conf.setString("taskmanager.network.memory.kind", "fixed");
        conf.setString("taskmanager.network.memory.min", "128mb");
        // 增加到128mb
        conf.setString("taskmanager.network.memory.max", "128mb");
        // 增加到128mb
        conf.setInteger("taskmanager.network.memory.buffers-per-channel", 2);
        // 减少每个通道的缓冲区数
        conf.setInteger("taskmanager.network.memory.floating-buffers-per-gate", 2);
        // 减少浮动缓冲区数
        env.configure(conf, EnhancedCommentAnalysisJob.class.getClassLoader());
        env.setParallelism(4);
        // 从默认的16降低到4
        // 1. 用户日志流
        DataStreamSource<String> logSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        KAFKA_BOOTSTRAP,
                        KAFKA_TOPIC_LOGS,
                        CONSUMER_GROUP_LOGS,
                        OffsetsInitializer.earliest()
                ),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafkaLogSource"
        );

        // 新增：对日志进行解析和 keyBy 得到 userLogStream
        KeyedStream<JSONObject, String> userLogStream = logSource
                .map(new MapUserImage())

                .filter(d -> d != null && d.getString("user_id") != null)
                .keyBy(d -> d.getString("user_id"));

        // 2. 用户信息流（来自 PostgreSQL）
        KeyedStream<JSONObject, String> userInfoStream = env
                .addSource(UserInfoPostgresSource.createSourceFunction())
                .name("postgres-user-info-source")
                .keyBy(d -> d.getString("user_id"));

        // 3. 评论流
        DataStreamSource<String> commentSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        KAFKA_BOOTSTRAP,
                        KAFKA_TOPIC_COMMENT,
                        CONSUMER_GROUP_COMMENT,
                        OffsetsInitializer.earliest()
                ),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafkaCommentSource"
        );

        KeyedStream<JSONObject, String> commentStream = commentSource
                .map(new MapComment())
                .filter(d -> d != null && d.getString("user_id") != null)
                .keyBy(d -> d.getString("user_id"));

        // 4. 三流关联处理
        // 先关联用户信息和日志
        DataStream<JSONObject> userInfoWithLogs = userInfoStream
                .connect(userLogStream)
                .keyBy(
                        userInfo -> userInfo.getString("user_id"),
                        userLog -> userLog.getString("user_id")
                )
                .process(new UserInfoWithLogsJoinProcessor())
                .name("user-info-logs-join");

        // 再关联评论数据
        DataStream<JSONObject> finalResult = userInfoWithLogs
                .keyBy(d -> d.getString("user_id"))
                .connect(commentStream)
                .keyBy(
                        userInfoLog -> userInfoLog.getString("user_id"),
                        comment -> comment.getString("user_id")
                )
                .process(new FinalJoinWithCommentProcessor())
                .name("final-comment-join");

        // 输出结果
        finalResult.print("FINAL_JOINED_RESULT");

        System.out.println("🚀 启动三流关联任务...");
        env.execute("Enhanced-Three-Stream-Join-Job");

    }
}


class UserInfoWithLogsJoinProcessor extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {
    private String getStringOrDefault(JSONObject obj, String key, String defaultValue) {
        String value = obj.getString(key);
        return value != null ? value : defaultValue;
    }
    private transient org.apache.flink.api.common.state.ValueState<JSONObject> userInfoState;
    private transient org.apache.flink.api.common.state.ValueState<JSONObject> userLogState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        org.apache.flink.api.common.state.ValueStateDescriptor<JSONObject> userInfoDesc =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("user-info", JSONObject.class);
        org.apache.flink.api.common.state.ValueStateDescriptor<JSONObject> userLogDesc =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("user-log", JSONObject.class);

        userInfoState = getRuntimeContext().getState(userInfoDesc);
        userLogState = getRuntimeContext().getState(userLogDesc);
    }

    @Override
    public void processElement1(JSONObject userInfo, Context ctx, Collector<JSONObject> out) throws Exception {
        userInfoState.update(userInfo);
        JSONObject userLog = userLogState.value();

        if (userLog != null) {
            JSONObject joined = joinUserInfoWithLog(userInfo, userLog);
            System.out.println("🔗 用户信息+日志关联成功: " + userInfo.getString("user_id"));
            out.collect(joined);
        }
    }

    @Override
    public void processElement2(JSONObject userLog, Context ctx, Collector<JSONObject> out) throws Exception {
        userLogState.update(userLog);
        JSONObject userInfo = userInfoState.value();

        if (userInfo != null) {
            JSONObject joined = joinUserInfoWithLog(userInfo, userLog);
            System.out.println("🔗 日志+用户信息关联成功: " + userLog.getString("user_id"));
            out.collect(joined);
        }
    }

    private JSONObject joinUserInfoWithLog(JSONObject userInfo, JSONObject userLog) {
        JSONObject result = new JSONObject();

        // 用户基本信息
        result.put("user_id", userInfo.getString("user_id"));
        result.put("uname", userInfo.getString("uname"));
        result.put("phone_num", userInfo.getString("phone_num"));
        result.put("birthday", userInfo.getString("birthday"));
        result.put("gender", userInfo.getString("gender"));
        result.put("address", userInfo.getString("address"));

        // 用户行为日志
        result.put("log_product_id", userLog.getString("product_id"));
        result.put("log_order_id", userLog.getString("order_id"));
        result.put("log_ts", userLog.getLong("ts"));

        result.put("log_type", getStringOrDefault(userLog, "log_type", "unknown"));

        result.put("join_type", "user_info_with_log");
        result.put("join_ts", System.currentTimeMillis());

        return result;
    }
}

class FinalJoinWithCommentProcessor extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {

    private transient org.apache.flink.api.common.state.ValueState<JSONObject> userInfoLogState;
    private transient org.apache.flink.api.common.state.ValueState<JSONObject> commentState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        org.apache.flink.api.common.state.ValueStateDescriptor<JSONObject> userInfoLogDesc =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("user-info-log", JSONObject.class);
        org.apache.flink.api.common.state.ValueStateDescriptor<JSONObject> commentDesc =
                new org.apache.flink.api.common.state.ValueStateDescriptor<>("comment", JSONObject.class);

        userInfoLogState = getRuntimeContext().getState(userInfoLogDesc);
        commentState = getRuntimeContext().getState(commentDesc);
    }

    @Override
    public void processElement1(JSONObject userInfoLog, Context ctx, Collector<JSONObject> out) throws Exception {
        userInfoLogState.update(userInfoLog);
        JSONObject comment = commentState.value();

        if (comment != null) {
            JSONObject finalJoined = finalJoin(userInfoLog, comment);
            System.out.println("🎯 最终关联成功(用户信息+日志+评论): " + userInfoLog.getString("user_id"));
            out.collect(finalJoined);
        }
    }

    @Override
    public void processElement2(JSONObject comment, Context ctx, Collector<JSONObject> out) throws Exception {
        commentState.update(comment);
        JSONObject userInfoLog = userInfoLogState.value();

        if (userInfoLog != null) {
            JSONObject finalJoined = finalJoin(userInfoLog, comment);
            System.out.println("🎯 最终关联成功(评论+用户信息+日志): " + comment.getString("user_id"));
            out.collect(finalJoined);
        }
    }

    private JSONObject finalJoin(JSONObject userInfoLog, JSONObject comment) {
        JSONObject result = new JSONObject();
        result.putAll(userInfoLog);

        // 评论数据
        result.put("comment_order_id", comment.getString("order_id"));
        result.put("comment_text", comment.getString("comment"));

        String commentLevel = comment.getString("comment_level");
        if (commentLevel == null) {
            commentLevel = "unknown";
        }
        result.put("comment_level", commentLevel);

        Integer isBlack = comment.getObject("is_black", Integer.class);
        if (isBlack == null) {
            isBlack = 0;
        }
        result.put("is_black", isBlack);

        result.put("sensitive_words", comment.getJSONArray("sensitive_words"));

        result.put("join_type", "final_three_stream_join");
        result.put("process_ts", System.currentTimeMillis());

        // 添加业务逻辑：风险等级分析
        if ("P0".equals(commentLevel) || (isBlack != null && isBlack == 1)) {
            result.put("risk_level", "high");
            result.put("needs_review", true);
        } else {
            result.put("risk_level", "low");
            result.put("needs_review", false);
        }

        return result;
    }
}