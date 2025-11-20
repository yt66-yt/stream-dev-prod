package com.stream.realtime.lululemon.flinkapi.todoris;

import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson2.JSON;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 用户路径分析任务 - 使用JDBC Sink写入Doris
 */
public class UserPathAnalysisTasktodoris {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    // Doris 配置 - 使用与页面访问量相同的配置
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 用于统计Kafka累计条数的全局变量
    private static long totalKafkaRecords = 0L;

    /**
     * Kafka日志数据实体类
     */
    @Data
    public static class LogData {
        private String log_id;
        private Device device;
        private Gis gis;
        private Network network;
        private String opa;
        private String log_type;
        private Long ts;
        private String product_id;
        private String order_id;
        private String user_id;
        private java.util.List<String> keywords;
    }

    @Data
    public static class Device {
        private String brand;
        private String plat;
        private String platv;
        private String softv;
        private String uname;
        private String userkey;
        private String device;
    }

    @Data
    public static class Gis {
        private String ip;
    }

    @Data
    public static class Network {
        private String net;
    }

    /**
     * 用户行为事件实体类
     */
    @Data
    public static class UserEvent {
        private String userId;
        private String eventType;
        private Long timestamp;
        private String date;
        private String logId;
        private String productId;
        private String orderId;
        private String brand;
        private String ip;

        public UserEvent() {}

        public UserEvent(String userId, String eventType, Long timestamp, String date,
                         String logId, String productId, String orderId, String brand, String ip) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
            this.date = date;
            this.logId = logId;
            this.productId = productId;
            this.orderId = orderId;
            this.brand = brand;
            this.ip = ip;
        }
    }

    /**
     * 用户路径记录实体类 - 对应Doris表结构
     */
    @Data
    public static class UserPathRecord {
        private String userId;           // 用户ID
        private String eventDate;        // 事件日期
        private String windowStartTime;  // 窗口开始时间
        private String pathSequence;     // 路径序列
        private Integer pathLength;      // 路径长度
        private String startEvent;       // 开始事件
        private String endEvent;         // 结束事件
        private String windowEndTime;    // 窗口结束时间
        private Timestamp processTime;   // 处理时间

        public UserPathRecord() {}

        public UserPathRecord(String userId, String eventDate, String windowStartTime,
                              String pathSequence, Integer pathLength,
                              String startEvent, String endEvent, String windowEndTime) {
            this.userId = userId;
            this.eventDate = eventDate;
            this.windowStartTime = windowStartTime;
            this.pathSequence = pathSequence;
            this.pathLength = pathLength;
            this.startEvent = startEvent;
            this.endEvent = endEvent;
            this.windowEndTime = windowEndTime;
            this.processTime = new Timestamp(System.currentTimeMillis());
        }
    }

    /**
     * 时间戳标准化和日期格式化方法
     */
    private static String normalizeAndFormatDate(Long timestamp) {
        if (timestamp == null) {
            return LocalDateTime.now().format(DATE_FORMATTER);
        }

        String timestampStr = String.valueOf(timestamp);
        long normalizedMillis;

        // 处理秒级时间戳和毫秒级时间戳
        if (timestampStr.length() == 10) {
            normalizedMillis = timestamp * 1000L;
        } else if (timestampStr.length() == 13) {
            normalizedMillis = timestamp;
        } else {
            normalizedMillis = System.currentTimeMillis();
        }

        try {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(normalizedMillis), ZoneId.systemDefault());
            return dateTime.format(DATE_FORMATTER);
        } catch (Exception e) {
            return LocalDateTime.now().format(DATE_FORMATTER);
        }
    }

    /**
     * 时间戳格式化方法
     */
    private static String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return LocalDateTime.now().format(TIME_FORMATTER);
        }

        String timestampStr = String.valueOf(timestamp);
        long normalizedMillis;

        if (timestampStr.length() == 10) {
            normalizedMillis = timestamp * 1000L;
        } else if (timestampStr.length() == 13) {
            normalizedMillis = timestamp;
        } else {
            normalizedMillis = System.currentTimeMillis();
        }

        try {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(normalizedMillis), ZoneId.systemDefault());
            return dateTime.format(TIME_FORMATTER);
        } catch (Exception e) {
            return LocalDateTime.now().format(TIME_FORMATTER);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("=== 启动用户路径分析作业 - 使用JDBC Sink写入Doris ===");

        // 1. 读取Kafka数据源
        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOOTSTRAP_SERVERS, KAFKA_LOG_TOPIC,
                        "user-path-analysis-group", OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafka_log_source"
        );

        // 2. 统计Kafka累计条数（借鉴页面访问量的统计方式）
        originKafkaLogDs
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        synchronized (UserPathAnalysisTasktodoris.class) {
                            totalKafkaRecords++;
                        }
                        return value;
                    }
                })
                .keyBy(value -> "total_count")
                .process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, String, String>() {
                    private org.apache.flink.api.common.state.ValueState<Boolean> timerRegistered;
                    private org.apache.flink.api.common.state.ValueState<Long> lastReportCount;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        timerRegistered = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("timerRegistered", Boolean.class));
                        lastReportCount = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("lastReportCount", Long.class));
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        if (timerRegistered.value() == null || !timerRegistered.value()) {
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000);
                            timerRegistered.update(true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        long currentTotal = totalKafkaRecords;
                        Long lastCount = lastReportCount.value();

                        if (lastCount == null || currentTotal > lastCount) {
                            out.collect(String.format("累计读取Kafka数据: %d 条", currentTotal));
                            lastReportCount.update(currentTotal);
                        }

                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000);
                    }
                })
                .print("Kafka数据统计");

        // 3. JSON解析
        SingleOutputStreamOperator<LogData> parsedStream = originKafkaLogDs
                .flatMap(new FlatMapFunction<String, LogData>() {
                    @Override
                    public void flatMap(String value, Collector<LogData> out) {
                        try {
                            LogData logData = JSON.parseObject(value, LogData.class);
                            out.collect(logData);
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + e.getMessage());
                        }
                    }
                })
                .name("json-parser");

        // 4. 提取用户行为事件
        SingleOutputStreamOperator<UserEvent> userEventStream = parsedStream
                .flatMap(new FlatMapFunction<LogData, UserEvent>() {
                    @Override
                    public void flatMap(LogData logData, Collector<UserEvent> out) {
                        if (logData.getUser_id() != null && logData.getTs() != null && logData.getLog_type() != null) {
                            String userId = logData.getUser_id();
                            String eventType = normalizeEventType(logData.getLog_type());
                            Long timestamp = logData.getTs();
                            String date = normalizeAndFormatDate(timestamp);
                            String logId = logData.getLog_id();
                            String productId = logData.getProduct_id();
                            String orderId = logData.getOrder_id();
                            String brand = logData.getDevice() != null ? logData.getDevice().getBrand() : "unknown";
                            String ip = logData.getGis() != null ? logData.getGis().getIp() : "unknown";

                            UserEvent userEvent = new UserEvent(userId, eventType, timestamp, date,
                                    logId, productId, orderId, brand, ip);
                            out.collect(userEvent);
                        }
                    }

                    private String normalizeEventType(String logType) {
                        if (logType == null) {
                            return "unknown";
                        }
                        switch (logType.toLowerCase()) {
                            case "login":
                            case "home":
                            case "search":
                            case "product_list":
                            case "product_detail":
                            case "payment":
                                return logType.toLowerCase();
                            default:
                                return "other";
                        }
                    }
                })
                .name("user-event-extractor");

        // 5. 按用户分组，构建用户路径
        SingleOutputStreamOperator<UserPathRecord> userPathStream = userEventStream
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent event) throws Exception {
                        return event.getUserId() + "|" + event.getDate();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new ProcessWindowFunction<UserEvent, UserPathRecord, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<UserEvent> elements,
                                        Collector<UserPathRecord> out) throws Exception {

                        List<UserEvent> events = new ArrayList<>();
                        for (UserEvent event : elements) {
                            events.add(event);
                        }

                        // 只有至少2个事件才构建路径
                        if (events.size() >= 2) {
                            // 按时间戳排序
                            events.sort(Comparator.comparing(UserEvent::getTimestamp));

                            // 构建路径序列 - 去除连续重复事件
                            StringBuilder pathBuilder = new StringBuilder();
                            List<String> uniqueEvents = new ArrayList<>();
                            String lastEvent = null;

                            for (UserEvent event : events) {
                                if (lastEvent == null || !event.getEventType().equals(lastEvent)) {
                                    pathBuilder.append(event.getEventType()).append("->");
                                    uniqueEvents.add(event.getEventType());
                                    lastEvent = event.getEventType();
                                }
                            }

                            // 移除最后的"->"
                            String path = pathBuilder.length() > 2 ?
                                    pathBuilder.substring(0, pathBuilder.length() - 2) : "";

                            String[] keyParts = key.split("\\|");
                            String userId = keyParts[0];
                            String date = keyParts.length > 1 ? keyParts[1] : "unknown";

                            String startEvent = uniqueEvents.isEmpty() ? "unknown" : uniqueEvents.get(0);
                            String endEvent = uniqueEvents.isEmpty() ? "unknown" :
                                    uniqueEvents.get(uniqueEvents.size() - 1);

                            // 创建用户路径记录
                            UserPathRecord userPath = new UserPathRecord(
                                    userId,
                                    date,
                                    LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()),
                                            ZoneId.systemDefault()).format(TIME_FORMATTER),
                                    path,
                                    uniqueEvents.size(),
                                    startEvent,
                                    endEvent,
                                    LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()),
                                            ZoneId.systemDefault()).format(TIME_FORMATTER)
                            );
                            out.collect(userPath);
                        }
                    }
                })
                .name("user-path-builder");

        // 6. 写入用户路径到Doris（借鉴页面访问量的JDBC Sink方式）
        userPathStream.addSink(JdbcSink.sink(
                "INSERT INTO user_path_analysis " +
                        "(user_id, event_date, window_start_time, path_sequence, path_length, " +
                        "start_event, end_event, window_end_time, process_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.getUserId());
                    ps.setString(2, record.getEventDate());
                    ps.setString(3, record.getWindowStartTime());
                    ps.setString(4, record.getPathSequence());
                    ps.setInt(5, record.getPathLength());
                    ps.setString(6, record.getStartEvent());
                    ps.setString(7, record.getEndEvent());
                    ps.setString(8, record.getWindowEndTime());
                    ps.setTimestamp(9, record.getProcessTime());
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
        )).name("user_path_to_doris");

//         7. 控制台输出用于监控
        userPathStream
                .map(new MapFunction<UserPathRecord, String>() {
                    @Override
                    public String map(UserPathRecord userPath) throws Exception {
                        return String.format("用户路径: userId=%s, date=%s, path=%s, length=%d",
                                userPath.getUserId(), userPath.getEventDate(),
                                userPath.getPathSequence(), userPath.getPathLength());
                    }
                })
                .print("user-path-output");

        // 8. 窗口统计信息输出
        userPathStream
                .map(new MapFunction<UserPathRecord, Long>() {
                    @Override
                    public Long map(UserPathRecord value) throws Exception {
                        return 1L;
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        long windowPathCount = 0L;
                        for (Long element : elements) {
                            windowPathCount += element;
                        }

                        long currentTotalKafkaRecords;
                        synchronized (UserPathAnalysisTasktodoris.class) {
                            currentTotalKafkaRecords = totalKafkaRecords;
                        }

                        String stats = String.format(
                                "=== 窗口统计 ===\n" +
                                        "窗口结束时间: %s\n" +
                                        "本窗口分析路径数: %d\n" +
                                        "Kafka累计读取条数: %d\n" +
                                        "======================",
                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                windowPathCount,
                                currentTotalKafkaRecords
                        );
                        out.collect(stats);
                    }
                })
                .print("window-statistics");

        env.execute("用户路径分析 - JDBC写入Doris");
    }
}