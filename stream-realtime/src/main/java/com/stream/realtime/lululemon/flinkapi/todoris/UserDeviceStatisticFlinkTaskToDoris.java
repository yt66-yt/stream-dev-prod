package com.stream.realtime.lululemon.flinkapi.todoris;

import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
import java.util.Date;

/**
 * 设备信息提取 - 添加Doris导入功能
 */
public class UserDeviceStatisticFlinkTaskToDoris {
    private static final String KAFKA_BOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // Doris 配置 - 与UserPathAnalysisTasktodoris保持一致
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

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

    // 设备信息输出类（包含用户ID）
    @Data
    public static class DeviceInfo {
        private String userId;
        private String date;
        private String platformType;
        private String brand;
        private String version;

        public DeviceInfo() {}

        public DeviceInfo(String userId, String date, String platformType, String brand, String version) {
            this.userId = userId;
            this.date = date;
            this.platformType = platformType;
            this.brand = brand;
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("用户: %s, 平台: %s, 品牌: %s, 版本: %s, 日期: %s",
                    userId, platformType, brand, version, date);
        }
    }

    /**
     * 设备信息Doris记录实体类 - 对应Doris表结构
     */
    @Data
    public static class DeviceInfoRecord {
        private String userId;           // 用户ID
        private String eventDate;        // 事件日期
        private String platformType;     // 平台类型
        private String brand;            // 设备品牌
        private String version;          // 版本信息
        private Timestamp processTime;   // 处理时间

        public DeviceInfoRecord() {}

        public DeviceInfoRecord(String userId, String eventDate, String platformType,
                                String brand, String version) {
            this.userId = userId;
            this.eventDate = eventDate;
            this.platformType = platformType;
            this.brand = brand;
            this.version = version;
            this.processTime = new Timestamp(System.currentTimeMillis());
        }
    }

    // 公共的时间戳处理方法
    private static String normalizeAndFormatDate(Long timestamp) {
        if (timestamp == null) {
            return LocalDateTime.now().format(DATE_FORMATTER);
        }

        // 判断时间戳格式
        String timestampStr = String.valueOf(timestamp);
        long normalizedMillis;

        if (timestampStr.length() == 10) {
            // 10位时间戳，认为是秒，转换为毫秒
            normalizedMillis = timestamp * 1000L;
        } else if (timestampStr.length() == 13) {
            // 13位时间戳，认为是毫秒，直接使用
            normalizedMillis = timestamp;
        } else {
            // 其他格式，使用当前时间
            normalizedMillis = System.currentTimeMillis();
        }

        try {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(normalizedMillis), ZoneId.systemDefault());
            return dateTime.format(DATE_FORMATTER);
        } catch (Exception e) {
            // 如果时间戳异常，返回当前日期
            return LocalDateTime.now().format(DATE_FORMATTER);
        }
    }

    // 平台分类方法
    // 平台分类方法
    private static String classifyPlatform(String plat) {
        if (plat == null) {
            return "unknown";
        }
        String platLower = plat.toLowerCase();
        if (platLower.contains("ios") || platLower.contains("ipad") || platLower.contains("iphone")) {
            return "iOS";
        } else if (platLower.contains("android")) {
            return "Android";
        } else if (platLower.contains("hmos")) {
            return "HarmonyOS";
        } else {
            return "其他";
        }
    }
    // 版本格式化方法
    private static String formatVersion(String platv, String softv) {
        if (platv == null) {
            platv = "unknown";
        }
        if (softv == null) {
            softv = "unknown";
        }
        return platv + "_" + softv;
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("=== 启动设备信息分析作业 - 使用JDBC Sink写入Doris ===");

        // 读取Kafka数据
        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_v3_logs"
        );

        // JSON解析器
        SingleOutputStreamOperator<LogData> parsedStream = originKafkaLogDs
                .flatMap(new FlatMapFunction<String, LogData>() {
                    @Override
                    public void flatMap(String value, Collector<LogData> out) {
                        try {
                            LogData logData = JSON.parseObject(value, LogData.class);
                            out.collect(logData);
                        } catch (Exception e) {
                            System.err.println("Failed to parse JSON: " + e.getMessage());
                        }
                    }
                })
                .name("json-parser");

        // 设备信息提取器 - 每条数据都输出（包含用户ID）
        SingleOutputStreamOperator<DeviceInfo> deviceStream = parsedStream
                .flatMap(new FlatMapFunction<LogData, DeviceInfo>() {
                    @Override
                    public void flatMap(LogData logData, Collector<DeviceInfo> out) {
                        if (logData.getDevice() != null && logData.getTs() != null && logData.getUser_id() != null) {
                            String userId = logData.getUser_id();
                            String platformType = classifyPlatform(logData.getDevice().getPlat());
                            String brand = logData.getDevice().getBrand();
                            String version = formatVersion(logData.getDevice().getPlatv(), logData.getDevice().getSoftv());
                            String date = normalizeAndFormatDate(logData.getTs());

                            DeviceInfo deviceInfo = new DeviceInfo(userId, date, platformType, brand, version);
                            out.collect(deviceInfo);
                        }
                    }
                })
                .name("device-info-extractor");

        // 转换为Doris记录格式
        SingleOutputStreamOperator<DeviceInfoRecord> dorisRecordStream = deviceStream
                .map(new MapFunction<DeviceInfo, DeviceInfoRecord>() {
                    @Override
                    public DeviceInfoRecord map(DeviceInfo deviceInfo) throws Exception {
                        return new DeviceInfoRecord(
                                deviceInfo.getUserId(),
                                deviceInfo.getDate(),
                                deviceInfo.getPlatformType(),
                                deviceInfo.getBrand(),
                                deviceInfo.getVersion()
                        );
                    }
                })
                .name("doris-record-converter");

        // 写入设备信息到Doris（借鉴UserPathAnalysisTasktodoris的JDBC Sink方式）
        dorisRecordStream.addSink(JdbcSink.sink(
                "INSERT INTO user_device_analysis " +
                        "(user_id, event_date, platform_type, brand, version, process_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.getUserId());
                    ps.setString(2, record.getEventDate());
                    ps.setString(3, record.getPlatformType());
                    ps.setString(4, record.getBrand());
                    ps.setString(5, record.getVersion());
                    ps.setTimestamp(6, record.getProcessTime());
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
        )).name("device_info_to_doris");

        // 输出每条设备信息（包含用户ID）
        deviceStream
                .map(new MapFunction<DeviceInfo, String>() {
                    @Override
                    public String map(DeviceInfo deviceInfo) throws Exception {
                        return "[设备信息] " + deviceInfo.toString();
                    }
                })
                .print("device-info-output");

        // 统计设备信息窗口输出数量
        deviceStream
                .map(new MapFunction<DeviceInfo, Long>() {
                    @Override
                    public Long map(DeviceInfo value) throws Exception {
                        return 1L;
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Long element : elements) {
                            count += element;
                        }

                        String windowInfo = String.format(
                                "=== 窗口统计 ===\n" +
                                        "窗口结束时间: %s\n" +
                                        "本窗口处理设备记录数: %d\n" +
                                        "======================",
                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                count
                        );

                        out.collect(windowInfo);
                    }
                })
                .print("window-statistics");

        // 添加Doris写入统计
        dorisRecordStream
                .map(new MapFunction<DeviceInfoRecord, String>() {
                    @Override
                    public String map(DeviceInfoRecord record) throws Exception {
                        return String.format("写入Doris设备记录: userId=%s, platform=%s, brand=%s",
                                record.getUserId(), record.getPlatformType(), record.getBrand());
                    }
                })
                .print("doris-write-output");

        env.execute("设备信息分析 - JDBC写入Doris");
    }
}