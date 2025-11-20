package com.stream.realtime.lululemon.flinkapi.todoris;

import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
 * 每日搜索关键词TOP10统计 - 使用JDBC Sink写入Doris（修复排名问题）
 */
public class SearchTermTotalAggregatorApitodoris {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";

    // Doris 配置
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 用于统计Kafka累计条数的全局变量
    private static long totalKafkaRecords = 0L;

    /**
     * 搜索日志数据实体类
     */
    @Data
    public static class SearchLog {
        private String logType;
        private Long ts;
        private List<String> keywords;
        private String userId;
        private String productId;
        private String orderId;
        private String rawData;
    }

    /**
     * 关键词事件实体类
     */
    @Data
    public static class KeywordEvent {
        private String keyword;
        private String date;
        private Long timestamp;

        public KeywordEvent() {}

        public KeywordEvent(String keyword, String date, Long timestamp) {
            this.keyword = keyword;
            this.date = date;
            this.timestamp = timestamp;
        }
    }

    /**
     * 关键词统计实体类
     */
    @Data
    public static class KeywordStats {
        private String keyword;
        private String date;
        private Long searchCount;
        private Long processTime;

        public KeywordStats() {}

        public KeywordStats(String keyword, String date, Long searchCount) {
            this.keyword = keyword;
            this.date = date;
            this.searchCount = searchCount;
            this.processTime = System.currentTimeMillis();
        }
    }

    /**
     * 关键词排名实体类
     */
    @Data
    public static class KeywordRank {
        private String keyword;
        private Long totalCount;
        private Integer rank;

        @Override
        public String toString() {
            return String.format("#%d %s (搜索次数: %d)", rank, keyword, totalCount);
        }
    }

    /**
     * 每日TOP10结果实体类
     */
    @Data
    public static class DailyTop10Result {
        private String date;
        private List<KeywordRank> top10Keywords;
        private Integer totalUniqueKeywords;
        private Integer totalProcessedRecords;
        private Long processTime;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n").append(generateSeparator(50));
            sb.append("\n           每日搜索词TOP10词云");
            sb.append("\n").append(generateSeparator(50));
            sb.append("\n日期: ").append(date);
            sb.append("\n总关键词数: ").append(totalUniqueKeywords);
            sb.append("\n处理记录数: ").append(totalProcessedRecords);
            sb.append("\n").append(generateSeparator(50));
            sb.append("\nTOP10搜索词:\n");

            for (KeywordRank rank : top10Keywords) {
                sb.append("   ").append(rank.toString()).append("\n");
            }

            sb.append(generateSeparator(50));
            sb.append("\n更新时间: ").append(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(processTime), ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            sb.append("\n").append(generateSeparator(50));
            return sb.toString();
        }

        private String generateSeparator(int length) {
            StringBuilder separator = new StringBuilder();
            for (int i = 0; i < length; i++) {
                separator.append("=");
            }
            return separator.toString();
        }
    }

    /**
     * TOP10记录实体类 - 对应Doris表结构
     */
    @Data
    public static class Top10Record {
        private String statDate;              // 统计日期
        private String keyword;               // 搜索关键词
        private Long totalCount;              // 总搜索次数
        private Integer dailyRank;            // 当日排名
        private Integer totalUniqueKeywords;  // 总关键词数
        private Integer totalProcessedRecords; // 处理记录数
        private Timestamp processTime;        // 处理时间
        private Timestamp updateTime;         // 更新时间

        public Top10Record() {}

        public Top10Record(String statDate, String keyword, Long totalCount, Integer dailyRank,
                           Integer totalUniqueKeywords, Integer totalProcessedRecords) {
            this.statDate = statDate;
            this.keyword = keyword;
            this.totalCount = totalCount;
            this.dailyRank = dailyRank;
            this.totalUniqueKeywords = totalUniqueKeywords;
            this.totalProcessedRecords = totalProcessedRecords;
            this.processTime = new Timestamp(System.currentTimeMillis());
            this.updateTime = new Timestamp(System.currentTimeMillis());
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

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        System.out.println("=== 启动每日搜索关键词TOP10统计作业 - 使用JDBC Sink写入Doris（修复排名问题） ===");

        // 1. 读取Kafka数据源
        DataStreamSource<String> originKafkaLogDs = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOOTSTRAP_SERVERS, KAFKA_LOG_TOPIC,
                        "search-keyword-top10-group", OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafka_search_log_source"
        );

        // 2. 统计Kafka累计条数
        originKafkaLogDs
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        synchronized (SearchTermTotalAggregatorApitodoris.class) {
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
        SingleOutputStreamOperator<SearchLog> parsedStream = originKafkaLogDs
                .flatMap(new FlatMapFunction<String, SearchLog>() {
                    @Override
                    public void flatMap(String value, Collector<SearchLog> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            SearchLog log = new SearchLog();
                            log.setLogType(jsonObject.getString("log_type"));

                            // 处理时间戳，兼容10位和13位
                            Object tsObj = jsonObject.get("ts");
                            if (tsObj != null) {
                                if (tsObj instanceof Long) {
                                    log.setTs((Long) tsObj);
                                } else if (tsObj instanceof Integer) {
                                    log.setTs(((Integer) tsObj).longValue());
                                } else {
                                    try {
                                        String tsStr = tsObj.toString();
                                        long ts = Long.parseLong(tsStr);
                                        log.setTs(ts);
                                    } catch (NumberFormatException e) {
                                        System.err.println("Invalid timestamp format: " + tsObj);
                                        log.setTs(System.currentTimeMillis() / 1000);
                                    }
                                }
                            } else {
                                log.setTs(System.currentTimeMillis() / 1000);
                            }

                            log.setRawData(value);

                            // 解析keywords数组
                            if (jsonObject.containsKey("keywords")) {
                                try {
                                    List<String> keywords = jsonObject.getJSONArray("keywords")
                                            .toList(String.class);
                                    log.setKeywords(keywords);
                                } catch (Exception e) {
                                    System.err.println("Keywords parse error: " + e.getMessage());
                                    log.setKeywords(null);
                                }
                            } else {
                                log.setKeywords(null);
                            }

                            log.setUserId(jsonObject.getString("user_id"));
                            log.setProductId(jsonObject.getString("product_id"));
                            log.setOrderId(jsonObject.getString("order_id"));
                            out.collect(log);
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + e.getMessage());
                        }
                    }
                })
                .name("json-parser");

        // 4. 提取搜索关键词
        SingleOutputStreamOperator<KeywordEvent> keywordStream = parsedStream
                .flatMap(new FlatMapFunction<SearchLog, KeywordEvent>() {
                    @Override
                    public void flatMap(SearchLog searchLog, Collector<KeywordEvent> out) {
                        if (searchLog.getKeywords() != null && !searchLog.getKeywords().isEmpty()
                                && "search".equals(searchLog.getLogType())) {
                            long timestamp = searchLog.getTs();
                            String date = normalizeAndFormatDate(timestamp);

                            for (String keyword : searchLog.getKeywords()) {
                                if (isValidKeyword(keyword)) {
                                    KeywordEvent event = new KeywordEvent(keyword.trim(), date, timestamp);
                                    out.collect(event);
                                }
                            }
                        }
                    }

                    private boolean isValidKeyword(String keyword) {
                        return keyword != null && !keyword.trim().isEmpty() && keyword.length() <= 100;
                    }
                })
                .name("keyword-extractor");

        // 5. 按日期和关键词分组统计搜索次数
        SingleOutputStreamOperator<KeywordStats> keywordStatsStream = keywordStream
                .keyBy(event -> event.getDate() + "|" + event.getKeyword())
                .process(new KeyedProcessFunction<String, KeywordEvent, KeywordStats>() {

                    private transient ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> countDescriptor =
                                new ValueStateDescriptor<>("countState", Types.LONG);
                        countState = getRuntimeContext().getState(countDescriptor);
                    }

                    @Override
                    public void processElement(KeywordEvent event, Context ctx, Collector<KeywordStats> out) throws Exception {
                        Long currentCount = countState.value();
                        if (currentCount == null) {
                            currentCount = 0L;
                        }
                        currentCount += 1;
                        countState.update(currentCount);

                        KeywordStats stats = new KeywordStats(event.getKeyword(), event.getDate(), currentCount);
                        out.collect(stats);
                    }
                })
                .name("keyword-stats-processor");

        // 6. 按日期分组，在全局范围内计算TOP10排名
        SingleOutputStreamOperator<DailyTop10Result> top10Stream = keywordStatsStream
                .keyBy(KeywordStats::getDate)
                .process(new KeyedProcessFunction<String, KeywordStats, DailyTop10Result>() {

                    private transient MapState<String, Long> keywordCounts;
                    private transient ValueState<Integer> processCounterState;
                    private transient ValueState<Long> lastOutputTimeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Long> countsDescriptor =
                                new MapStateDescriptor<>("keywordCounts", Types.STRING, Types.LONG);
                        keywordCounts = getRuntimeContext().getMapState(countsDescriptor);

                        ValueStateDescriptor<Integer> counterDescriptor =
                                new ValueStateDescriptor<>("processCounter", Types.INT);
                        processCounterState = getRuntimeContext().getState(counterDescriptor);

                        ValueStateDescriptor<Long> timeDescriptor =
                                new ValueStateDescriptor<>("lastOutputTime", Types.LONG);
                        lastOutputTimeState = getRuntimeContext().getState(timeDescriptor);
                    }

                    @Override
                    public void processElement(KeywordStats stats, Context ctx, Collector<DailyTop10Result> out) throws Exception {
                        // 更新关键词计数
                        keywordCounts.put(stats.getKeyword(), stats.getSearchCount());

                        // 更新计数器
                        Integer processCount = processCounterState.value();
                        if (processCount == null) {
                            processCount = 0;
                        }
                        processCount++;
                        processCounterState.update(processCount);

                        // 获取上次输出时间
                        Long lastOutputTime = lastOutputTimeState.value();
                        long currentTime = System.currentTimeMillis();

                        // 每处理30条记录或每30秒输出一次TOP10
                        if (processCount % 30 == 0 ||
                                lastOutputTime == null ||
                                currentTime - lastOutputTime > 30000) {

                            outputTop10Result(stats.getDate(), out, processCount);
                            lastOutputTimeState.update(currentTime);
                        }
                    }

                    private void outputTop10Result(String date, Collector<DailyTop10Result> out, int processCount) throws Exception {
                        // 获取该日期所有关键词并按计数排序
                        List<Map.Entry<String, Long>> allKeywords = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : keywordCounts.entries()) {
                            allKeywords.add(entry);
                        }

                        // 按计数降序排序
                        allKeywords.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

                        // 取TOP10并设置正确的排名
                        List<KeywordRank> top10 = new ArrayList<>();
                        for (int i = 0; i < Math.min(10, allKeywords.size()); i++) {
                            Map.Entry<String, Long> entry = allKeywords.get(i);
                            KeywordRank rank = new KeywordRank();
                            rank.setKeyword(entry.getKey());
                            rank.setTotalCount(entry.getValue());
                            rank.setRank(i + 1);  // 正确的全局排名
                            top10.add(rank);
                        }

                        // 创建TOP10结果
                        DailyTop10Result result = new DailyTop10Result();
                        result.setDate(date);
                        result.setTop10Keywords(top10);
                        result.setTotalUniqueKeywords(allKeywords.size());
                        result.setTotalProcessedRecords(processCount);
                        result.setProcessTime(System.currentTimeMillis());

                        out.collect(result);

                        // 输出调试信息
                        System.out.println("日期 " + date + " 的TOP10排名计算完成:");
                        for (KeywordRank rank : top10) {
                            System.out.println("  排名 " + rank.getRank() + ": " + rank.getKeyword() + " (" + rank.getTotalCount() + "次)");
                        }
                    }
                })
                .name("top10-processor");

        // 7. 将TOP10结果转换为Doris记录格式
        SingleOutputStreamOperator<Top10Record> dorisRecordStream = top10Stream
                .flatMap(new FlatMapFunction<DailyTop10Result, Top10Record>() {
                    @Override
                    public void flatMap(DailyTop10Result top10Result, Collector<Top10Record> out) throws Exception {
                        if (top10Result.getTop10Keywords() != null && !top10Result.getTop10Keywords().isEmpty()) {
                            for (KeywordRank rank : top10Result.getTop10Keywords()) {
                                Top10Record record = new Top10Record(
                                        top10Result.getDate(),
                                        rank.getKeyword(),
                                        rank.getTotalCount(),
                                        rank.getRank(),  // 这里现在使用正确的全局排名
                                        top10Result.getTotalUniqueKeywords(),
                                        top10Result.getTotalProcessedRecords()
                                );
                                out.collect(record);
                            }

                            System.out.println("转换 " + top10Result.getTop10Keywords().size() +
                                    " 条TOP10记录，日期: " + top10Result.getDate());
                        }
                    }
                })
                .name("doris-record-converter");

        // 8. 写入TOP10结果到Doris
        dorisRecordStream.addSink(JdbcSink.sink(
                "INSERT INTO search_keyword_top10_daily " +
                        "(stat_date, keyword, total_count, daily_rank, total_unique_keywords, " +
                        "total_processed_records, process_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.getStatDate());
                    ps.setString(2, record.getKeyword());
                    ps.setLong(3, record.getTotalCount());
                    ps.setInt(4, record.getDailyRank());
                    ps.setInt(5, record.getTotalUniqueKeywords());
                    ps.setInt(6, record.getTotalProcessedRecords());
                    ps.setTimestamp(7, record.getProcessTime());
                    ps.setTimestamp(8, record.getUpdateTime());
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
        )).name("top10_to_doris");

        // 9. 控制台输出用于监控
        top10Stream
                .map(new MapFunction<DailyTop10Result, String>() {
                    @Override
                    public String map(DailyTop10Result result) throws Exception {
                        return String.format("TOP10结果: date=%s, totalKeywords=%d, processedRecords=%d",
                                result.getDate(), result.getTotalUniqueKeywords(), result.getTotalProcessedRecords());
                    }
                })
                .print("top10-output");

        env.execute("每日搜索关键词TOP10统计 - JDBC写入Doris（修复排名问题）");
    }
}