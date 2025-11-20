package com.stream.realtime.lululemon.flinkapi.todoris;

import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 实时统计：每日页面访问量 - 写入Doris
 */
public class EachPageVisitCountApitodoris {
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    // Doris 配置
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 替代 String.repeat() 的方法
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 禁用检查点以避免HDFS问题
        env.getCheckpointConfig().disableCheckpointing();

        System.out.println("=== 启动每日页面访问量统计作业 - 写入Doris ===");

        // 1. 从 Kafka 读取数据源
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC,
                        "each-page-visit-count-group", OffsetsInitializer.earliest()),
                WatermarkStrategy.forMonotonousTimestamps(),
                "kafka_log_source"
        );

        // 2. 数据解析和转换
        DataStream<Tuple3<String, String, Long>> pageViewStream = kafkaSource
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String logType = json.getString("log_type");
                            Long timestamp = json.getLong("ts");

                            if (logType != null && timestamp != null) {
                                // 检查时间戳是否合理（毫秒或秒级时间戳）
                                long actualTimestamp;
                                if (timestamp > 1000000000000L) {
                                    // 毫秒级时间戳
                                    actualTimestamp = timestamp;
                                } else {
                                    // 秒级时间戳，转换为毫秒
                                    actualTimestamp = timestamp * 1000L;
                                }

                                // 转换为日期格式
                                LocalDateTime dateTime = LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(actualTimestamp), ZoneId.systemDefault());
                                String date = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                                // 输出: (页面类型, 日期, 1)
                                out.collect(new Tuple3<>(logType, date, 1L));
                            }
                        } catch (Exception e) {
                            // 解析错误忽略
                        }
                    }
                });

        // 累计Kafka数据量统计 - 使用状态管理实现真正的全局累计
        kafkaSource
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return new Tuple2<>("total_kafka_count", 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    private ValueState<Long> totalCountState;
                    private ValueState<Boolean> timerRegistered;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态，存储累计Kafka数据量
                        ValueStateDescriptor<Long> descriptor =
                                new ValueStateDescriptor<>("kafkaTotalCountState", Long.class);
                        totalCountState = getRuntimeContext().getState(descriptor);

                        ValueStateDescriptor<Boolean> timerDescriptor =
                                new ValueStateDescriptor<>("timerRegisteredState", Boolean.class);
                        timerRegistered = getRuntimeContext().getState(timerDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        // 获取当前状态值，如果没有则为0
                        Long currentCount = totalCountState.value();
                        if (currentCount == null) {
                            currentCount = 0L;
                        }

                        // 累加新值
                        Long newCount = currentCount + value.f1;
                        totalCountState.update(newCount);

                        // 只在第一次处理数据时注册定时器
                        if (timerRegistered.value() == null || !timerRegistered.value()) {
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000);
                            timerRegistered.update(true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        Long totalCount = totalCountState.value();
                        if (totalCount != null) {
                            out.collect(String.format("累计读取Kafka数据: %d 条", totalCount));
                        }

                        // 重新注册定时器，实现周期性输出
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000);
                    }
                })
                .print("累计Kafka数据量");

        // 3. 计算当天每个页面的访问量（按天统计）- 按日期分组输出
        DataStream<Tuple3<String, String, Long>> dailyPageViewCounts = pageViewStream
                .keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        // 累加计数
                        return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                });

        // 4. 写入每日页面访问量到Doris
        dailyPageViewCounts.addSink(JdbcSink.sink(
                "INSERT INTO daily_page_visit " +
                        "(log_type, visit_date, visit_count, update_time) " +
                        "VALUES (?, ?, ?, ?)",
                (ps, tuple) -> {
                    ps.setString(1, tuple.f0); // log_type
                    ps.setString(2, tuple.f1); // visit_date
                    ps.setLong(3, tuple.f2);   // visit_count
                    ps.setTimestamp(4, new Timestamp(System.currentTimeMillis())); // update_time
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USER)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        )).name("daily_page_visit_to_doris");

        // 5. 添加Doris写入调试输出
        dailyPageViewCounts
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        return String.format("准备写入Doris: log_type=%s, visit_date=%s, visit_count=%d",
                                value.f0, value.f1, value.f2);
                    }
                })
                .print("Doris写入数据");

        // 6. 优化输出格式 - 按日期和页面类型分组显示

        // 6.1 每日统计按日期分组输出
        dailyPageViewCounts
                .keyBy(value -> value.f1)
                // 按日期分组
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Long>, String>() {
                    private transient Map<String, List<Tuple3<String, String, Long>>> dateData;
                    private transient Map<String, Boolean> timerRegisteredMap;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        dateData = new HashMap<>();
                        timerRegisteredMap = new HashMap<>();
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Long> value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        String date = value.f1;
                        dateData.computeIfAbsent(date, k -> new ArrayList<>()).add(value);

                        // 只为每个日期注册一次定时器
                        if (!timerRegisteredMap.containsKey(date) || !timerRegisteredMap.get(date)) {
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                            timerRegisteredMap.put(date, true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        String date = ctx.getCurrentKey();
                        List<Tuple3<String, String, Long>> dataList = dateData.get(date);

                        if (dataList != null && !dataList.isEmpty()) {
                            // 按访问量排序
                            dataList.sort((a, b) -> Long.compare(b.f2, a.f2));

                            StringBuilder sb = new StringBuilder();
                            sb.append("\n").append(repeatString("=", 60)).append("\n");
                            sb.append("日期: ").append(date).append(" - 页面访问统计\n");
                            sb.append(repeatString("-", 60)).append("\n");

                            for (Tuple3<String, String, Long> data : dataList) {
                                sb.append(String.format("%-15s : %6d 次\n", data.f0, data.f2));
                            }

                            sb.append(repeatString("=", 60)).append("\n");
                            out.collect(sb.toString());

                            // 清空数据并重置定时器状态
                            dateData.remove(date);
                            timerRegisteredMap.remove(date);
                        }
                    }
                })
                .print("每日页面访问统计");

        env.execute("实时页面访问量统计任务 - 写入Doris");
    }
}