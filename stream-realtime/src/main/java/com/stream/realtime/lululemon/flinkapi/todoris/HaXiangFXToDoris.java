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
 * 用户画像分析 - 使用JDBC Sink写入Doris
 */
public class HaXiangFXToDoris {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    // Doris 配置 - 使用与用户路径分析相同的配置
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 用于统计Kafka累计条数的全局变量
    private static long totalKafkaRecords = 0L;

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

    // 用户行为分析结果 - 对应Doris表结构
    @Data
    public static class UserBehaviorAnalysis {
        private String userId;
        private String loginDates; // JSON数组字符串
        private Integer loginDaysCount;
        private Boolean hasPurchase;
        private Boolean hasSearch;
        private Boolean hasBrowse;
        private String loginTimeRanges; // JSON数组字符串
        private String networks; // JSON数组字符串
        private Timestamp analysisTime;
        private String windowStartTime;
        private String windowEndTime;

        public UserBehaviorAnalysis() {
            this.analysisTime = new Timestamp(System.currentTimeMillis());
        }

        public UserBehaviorAnalysis(String userId, List<String> loginDates, boolean hasPurchase,
                                    boolean hasSearch, boolean hasBrowse, List<String> loginTimeRanges,
                                    Set<String> networks, String windowStartTime, String windowEndTime) {
            this.userId = userId;
            this.loginDates = JSON.toJSONString(loginDates);
            this.loginDaysCount = loginDates.size();
            this.hasPurchase = hasPurchase;
            this.hasSearch = hasSearch;
            this.hasBrowse = hasBrowse;
            this.loginTimeRanges = JSON.toJSONString(loginTimeRanges);
            this.networks = JSON.toJSONString(new ArrayList<>(networks));
            this.analysisTime = new Timestamp(System.currentTimeMillis());
            this.windowStartTime = windowStartTime;
            this.windowEndTime = windowEndTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "用户ID: %s, 登录天数: %d天, 购买: %s, 搜索: %s, 浏览: %s, 登录时段: %s, 网络: %s",
                    userId, loginDaysCount,
                    hasPurchase ? "是" : "否", hasSearch ? "是" : "否", hasBrowse ? "是" : "否",
                    loginTimeRanges, networks
            );
        }
    }

    // 用户行为事件
    @Data
    public static class UserBehaviorEvent {
        private String userId;
        private String eventType;
        private String date;
        private String time;
        private Long timestamp;
        private String network;

        public UserBehaviorEvent() {}

        public UserBehaviorEvent(String userId, String eventType, String date, String time, Long timestamp, String network) {
            this.userId = userId;
            this.eventType = eventType;
            this.date = date;
            this.time = time;
            this.timestamp = timestamp;
            this.network = network;
        }
    }

    // 公共的时间戳处理方法
    private static String normalizeAndFormatDate(Long timestamp) {
        if (timestamp == null) {
            return LocalDateTime.now().format(DATE_FORMATTER);
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
            return dateTime.format(DATE_FORMATTER);
        } catch (Exception e) {
            return LocalDateTime.now().format(DATE_FORMATTER);
        }
    }

    private static String normalizeAndFormatTime(Long timestamp) {
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

    private static String getTimeRange(String time) {
        if (time == null) {
            return "未知";
        }

        try {
            int hour = Integer.parseInt(time.substring(0, 2));
            if (hour >= 5 && hour < 8) {
                return "清晨";
            } else if (hour >= 8 && hour < 12) {
                return "上午";
            } else if (hour >= 12 && hour < 14) {
                return "中午";
            } else if (hour >= 14 && hour < 18) {
                return "下午";
            } else if (hour >= 18 && hour < 22) {
                return "晚上";
            } else {
                return "深夜";
            }
        } catch (Exception e) {
            return "未知";
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("=== 启动用户画像分析作业 - 使用JDBC Sink写入Doris ===");

        // 1. 读取Kafka数据源
        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOOTSTRAP_SERVERS, KAFKA_LOG_TOPIC,
                        "user-behavior-analysis-group", OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafka_log_source"
        );

        // 2. 统计Kafka累计条数
        originKafkaLogDs
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        synchronized (HaXiangFXToDoris.class) {
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
        SingleOutputStreamOperator<UserBehaviorEvent> userEventStream = parsedStream
                .flatMap(new FlatMapFunction<LogData, UserBehaviorEvent>() {
                    @Override
                    public void flatMap(LogData logData, Collector<UserBehaviorEvent> out) {
                        if (logData.getUser_id() != null && logData.getTs() != null && logData.getLog_type() != null) {
                            String userId = logData.getUser_id();
                            String eventType = normalizeEventType(logData.getLog_type());
                            String date = normalizeAndFormatDate(logData.getTs());
                            String time = normalizeAndFormatTime(logData.getTs());
                            Long timestamp = logData.getTs();
                            String network = logData.getNetwork() != null ? logData.getNetwork().getNet() : "unknown";

                            UserBehaviorEvent userEvent = new UserBehaviorEvent(userId, eventType, date, time, timestamp, network);
                            out.collect(userEvent);
                        }
                    }

                    private String normalizeEventType(String logType) {
                        if (logType == null) {
                            return "unknown";
                        }
                        switch (logType.toLowerCase()) {
                            case "login":
                                return "login";
                            case "payment":
                                return "payment";
                            case "search":
                                return "search";
                            case "product_list":
                            case "product_detail":
                                return "browse";
                            case "home":
                                return "home";
                            default:
                                return "other";
                        }
                    }
                })
                .name("user-event-extractor");

        // 5. 按用户分组，分析用户行为
        SingleOutputStreamOperator<UserBehaviorAnalysis> userAnalysisStream = userEventStream
                .keyBy(new KeySelector<UserBehaviorEvent, String>() {
                    @Override
                    public String getKey(UserBehaviorEvent event) throws Exception {
                        return event.getUserId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new ProcessWindowFunction<UserBehaviorEvent, UserBehaviorAnalysis, String, TimeWindow>() {
                    @Override
                    public void process(String userId, Context context, Iterable<UserBehaviorEvent> elements,
                                        Collector<UserBehaviorAnalysis> out) throws Exception {

                        Set<String> loginDates = new HashSet<>();
                        boolean hasPurchase = false;
                        boolean hasSearch = false;
                        boolean hasBrowse = false;
                        Set<String> loginTimeRanges = new HashSet<>();
                        Set<String> networks = new HashSet<>();

                        for (UserBehaviorEvent event : elements) {
                            // 记录登录日期
                            if ("login".equals(event.getEventType())) {
                                loginDates.add(event.getDate());
                                // 记录登录时间段
                                String timeRange = getTimeRange(event.getTime());
                                loginTimeRanges.add(timeRange);
                            }

                            // 检查购买行为
                            if ("payment".equals(event.getEventType())) {
                                hasPurchase = true;
                            }

                            // 检查搜索行为
                            if ("search".equals(event.getEventType())) {
                                hasSearch = true;
                            }

                            // 检查浏览行为
                            if ("browse".equals(event.getEventType())) {
                                hasBrowse = true;
                            }

                            // 记录网络类型
                            if (event.getNetwork() != null && !"unknown".equals(event.getNetwork())) {
                                networks.add(event.getNetwork());
                            }
                        }

                        // 如果有登录行为，才输出分析结果
                        if (!loginDates.isEmpty()) {
                            // 转换为有序列表
                            List<String> sortedLoginDates = new ArrayList<>(loginDates);
                            Collections.sort(sortedLoginDates);

                            List<String> sortedTimeRanges = new ArrayList<>(loginTimeRanges);
                            Collections.sort(sortedTimeRanges);

                            String windowStartTime = LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(context.window().getStart()),
                                    ZoneId.systemDefault()).format(DATETIME_FORMATTER);
                            String windowEndTime = LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(context.window().getEnd()),
                                    ZoneId.systemDefault()).format(DATETIME_FORMATTER);

                            UserBehaviorAnalysis analysis = new UserBehaviorAnalysis(
                                    userId, sortedLoginDates, hasPurchase, hasSearch, hasBrowse,
                                    sortedTimeRanges, networks, windowStartTime, windowEndTime
                            );

                            out.collect(analysis);
                        }
                    }
                })
                .name("user-behavior-analyzer");

        // 6. 写入用户行为分析结果到Doris
        userAnalysisStream.addSink(JdbcSink.sink(
                "INSERT INTO user_behavior_analysis " +
                        "(user_id, login_dates, login_days_count, has_purchase, has_search, " +
                        "has_browse, login_time_ranges, networks, analysis_time, window_start_time, window_end_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, analysis) -> {
                    ps.setString(1, analysis.getUserId());
                    ps.setString(2, analysis.getLoginDates());
                    ps.setInt(3, analysis.getLoginDaysCount());
                    ps.setBoolean(4, analysis.getHasPurchase());
                    ps.setBoolean(5, analysis.getHasSearch());
                    ps.setBoolean(6, analysis.getHasBrowse());
                    ps.setString(7, analysis.getLoginTimeRanges());
                    ps.setString(8, analysis.getNetworks());
                    ps.setTimestamp(9, analysis.getAnalysisTime());
                    ps.setString(10, analysis.getWindowStartTime());
                    ps.setString(11, analysis.getWindowEndTime());
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
        )).name("user_behavior_to_doris");

        // 7. 控制台输出用于监控
        userAnalysisStream
                .map(new MapFunction<UserBehaviorAnalysis, String>() {
                    @Override
                    public String map(UserBehaviorAnalysis analysis) throws Exception {
                        return analysis.toString();
                    }
                })
                .print("user-behavior-output");

        // 8. 窗口统计信息输出
        userAnalysisStream
                .map(new MapFunction<UserBehaviorAnalysis, Long>() {
                    @Override
                    public Long map(UserBehaviorAnalysis value) throws Exception {
                        return 1L;
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        long windowAnalysisCount = 0L;
                        for (Long element : elements) {
                            windowAnalysisCount += element;
                        }

                        long currentTotalKafkaRecords;
                        synchronized (HaXiangFXToDoris.class) {
                            currentTotalKafkaRecords = totalKafkaRecords;
                        }

                        String stats = String.format(
                                "=== 用户画像窗口统计 ===\n" +
                                        "窗口结束时间: %s\n" +
                                        "本窗口分析用户数: %d\n" +
                                        "Kafka累计读取条数: %d\n" +
                                        "======================",
                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                windowAnalysisCount,
                                currentTotalKafkaRecords
                        );
                        out.collect(stats);
                    }
                })
                .print("window-statistics");

        env.execute("用户画像分析 - JDBC写入Doris");
    }
}