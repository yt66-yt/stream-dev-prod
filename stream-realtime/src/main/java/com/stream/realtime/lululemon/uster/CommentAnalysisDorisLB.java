package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class CommentAnalysisDorisLB {

    private static final String KAFKA_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_TOPIC2 = "realtime_v3_order_comment";
    private static final String KAFKA_BOOTSTRAP = "172.24.158.53:9092";
    private static final String CONSUMER_GROUP = "DBusUserProtraitLabel";
    private static final String CONSUMER_GROUP2 = "order_comment_processed";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        /********** 用户日志画像流 **********/
        DataStreamSource<String> originKafka = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        KAFKA_BOOTSTRAP,
                        KAFKA_TOPIC,
                        CONSUMER_GROUP,
                        OffsetsInitializer.earliest()
                ),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafkaSource"
        );

        // Map user image
        SingleOutputStreamOperator<JSONObject> mapped =
                originKafka
                        .map(new MapUserImage())
                        .filter(d -> d != null)
                        .filter(d -> {
                            if (d == null) {
                                return false;
                            }
                            String userId = d.getString("user_id");
                            return userId != null && !userId.trim().isEmpty();
                        })
                        .uid("portrait-stream");

        /********** 评论流 **********/
        DataStreamSource<String> commentProcessed = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        KAFKA_BOOTSTRAP,
                        KAFKA_TOPIC2,
                        CONSUMER_GROUP2,
                        OffsetsInitializer.earliest()
                ),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "commentSource"
        );

        SingleOutputStreamOperator<JSONObject> commentStream =
                commentProcessed
                        .map(new MapComment())
                        .filter(d -> {
                            if (d == null) {
                                return false;
                            }
                            String userId = d.getString("user_id");
                            return userId != null && !userId.trim().isEmpty();
                        });

        /********** 异步关联PostgreSQL用户信息 **********/
        // 为用户流关联用户信息
        DataStream<JSONObject> userWithInfo = AsyncDataStream
                .unorderedWait(mapped, new PostgreSQLAsyncFunction(), 5000, TimeUnit.MILLISECONDS, 100);

        // 为评论流关联用户信息
        DataStream<JSONObject> commentWithInfo = AsyncDataStream
                .unorderedWait(commentStream, new PostgreSQLAsyncFunction(), 5000, TimeUnit.MILLISECONDS, 100);

        /********** Join **********/
        DataStream<JSONObject> joined =
                UserPortraitCommentJoinProcessor.process(userWithInfo, commentWithInfo);

// 添加详细的输出处理
        joined.map(joinedResult -> {
            System.out.println("🔥 FINAL OUTPUT - JOINED RESULT:");
            System.out.println("   user_id: " + joinedResult.getString("user_id"));
            System.out.println("   uname: " + joinedResult.getString("uname"));
            System.out.println("   comment_text: " + joinedResult.getString("comment_text"));
            System.out.println("   join_type: " + joinedResult.getString("join_type"));
            System.out.println("   FULL DATA: " + joinedResult.toJSONString());
            return joinedResult;
        }).print("JOINED");

// 也可以保存到文件（用于调试）
        joined.writeAsText("file:///tmp/joined_results.txt")
                .name("debug-output");

        env.execute("Portrait-Comment-Join-With-PostgreSQL");
    }
}