package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.flinkcomment.util.IKAnalyzerUtil;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CommentAnalysisKafkaIK {

    /**
     * 确保Kafka主题存在
     */
    private static void ensureKafkaTopicExists(String bootstrapServers, String topicName) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> topics = adminClient.listTopics().names().get(30000, TimeUnit.MILLISECONDS);

            if (!topics.contains(topicName)) {
                System.out.println("⚠️  Kafka主题 '" + topicName + "' 不存在，正在创建...");

                Map<String, String> topicConfigs = new HashMap<>();
                topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, "604800000"); // 7天保留
                topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
                topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10485760"); // 10MB最大消息大小

                NewTopic newTopic = new NewTopic(topicName, 3, (short) 1)
                        .configs(topicConfigs);

                adminClient.createTopics(Collections.singletonList(newTopic)).all().get(30000, TimeUnit.MILLISECONDS);
                System.out.println("✅  Kafka主题 '" + topicName + "' 创建成功，分区数: 3, 副本因子: 1");
            } else {
                System.out.println("✅  Kafka主题 '" + topicName + "' 已存在");
            }
        } catch (Exception e) {
            System.err.println("❌  检查/创建Kafka主题失败: " + e.getMessage());
            System.out.println("⚠️  请确保Kafka broker配置允许自动创建主题，或手动创建主题: " + topicName);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置参数
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        EnvironmentSettingUtils.defaultParameter(env);

        // 确保Kafka主题存在
        String bootstrapServers = "172.24.158.53:9092";
        String topicName = "realtime_v3_order_comment";
        ensureKafkaTopicExists(bootstrapServers, topicName);

        // SQL Server CDC 源
        Properties debeziumProps = new Properties();
        debeziumProps.put("decimal.handling.mode", "string");

        DebeziumSourceFunction<String> sqlServerSource =
                SqlServerSource.<String>builder()
                        .hostname("192.168.200.30")
                        .port(1433)
                        .database("realtime_v3")
                        .tableList("dbo.jd_product_comments")
                        .username("sa")
                        .password("zhangyihao@123")
                        .debeziumProperties(debeziumProps)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        DataStreamSource<String> sourceStream = env.addSource(sqlServerSource, "SqlServer-Source");

        // 读取敏感词文件
        String p0SensitiveWordFile = "D:\\idealwj\\stream-dev-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\flinkcomment\\suspected-sensitive-words.txt";
        List<String> p0SensitiveWords = Files.lines(Paths.get(p0SensitiveWordFile))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());

        String p1SensitiveWordFile = "D:\\idealwj\\stream-dev-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\flinkcomment\\p1-sensitive-words.txt";
        List<String> p1SensitiveWords = Files.lines(Paths.get(p1SensitiveWordFile))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());

        System.out.println("已加载P0敏感词数量: " + p0SensitiveWords.size());
        System.out.println("已加载P1敏感词数量: " + p1SensitiveWords.size());

        if (!p0SensitiveWords.isEmpty()) {
            System.out.println("P0敏感词示例: " + p0SensitiveWords.subList(0, Math.min(5, p0SensitiveWords.size())));
        }
        if (!p1SensitiveWords.isEmpty()) {
            System.out.println("P1敏感词示例: " + p1SensitiveWords.subList(0, Math.min(5, p1SensitiveWords.size())));
        }

        // 合并所有敏感词用于预加载
        List<String> allSensitiveWords = new ArrayList<>();
        allSensitiveWords.addAll(p0SensitiveWords);
        allSensitiveWords.addAll(p1SensitiveWords);

        // 预加载敏感词到分词器
        IKAnalyzerUtil.preloadSensitiveWords(allSensitiveWords);
        System.out.println("✅ 敏感词预加载完成，总数: " + allSensitiveWords.size());

        // 测试分词效果
        testIKAnalyzer();

        // 创建包含级别信息的敏感词列表
        List<SensitiveWord> allSensitiveWordsWithLevel = new ArrayList<>();
        for (String word : p0SensitiveWords) {
            allSensitiveWordsWithLevel.add(new SensitiveWord(word, "P0"));
        }
        for (String word : p1SensitiveWords) {
            allSensitiveWordsWithLevel.add(new SensitiveWord(word, "P1"));
        }

        MapStateDescriptor<String, SensitiveWord> sensitiveStateDescriptor =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        // 创建广播流包含所有敏感词
        BroadcastStream<SensitiveWord> broadcastWords = env.fromCollection(allSensitiveWordsWithLevel)
                .broadcast(sensitiveStateDescriptor);

        // 数据处理流程
        KeyedStream<String, String> keyedStream = sourceStream.keyBy(json -> {
            try {
                JSONObject obj = JSON.parseObject(json);
                JSONObject after = obj.getJSONObject("after");
                return after != null ? after.getString("user_id") : "unknown";
            } catch (Exception e) {
                return "unknown";
            }
        });

        // 连接广播流进行处理
        SingleOutputStreamOperator<CommentResult> processedStream = keyedStream
                .connect(broadcastWords)
                .process(new CommentLevelProcessor())
                .name("comment-level-processor");

        // Kafka 配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProps.setProperty("batch.size", "16384");
        kafkaProps.setProperty("linger.ms", "5");
        kafkaProps.setProperty("buffer.memory", "33554432");
        kafkaProps.setProperty("acks", "1"); // 确保消息被确认
        kafkaProps.setProperty("retries", "3"); // 重试次数
        kafkaProps.setProperty("request.timeout.ms", "30000"); // 请求超时时间

        // 创建Kafka Sink
        FlinkKafkaProducer<CommentResult> kafkaSink = new FlinkKafkaProducer<>(
                topicName,
                new CommentResultKafkaSerializer(),
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        // 写入 Kafka
        processedStream.addSink(kafkaSink)
                .name("kafka-comment-sink");

        System.out.println("🚀 开始执行Flink作业: SQLServer Comment to Kafka with Level Classification");
        env.execute("SQLServer Comment to Kafka with Level Classification");
    }

    /**
     * Kafka序列化器 - 将CommentResult对象序列化为JSON字符串
     */
    public static class CommentResultKafkaSerializer implements KafkaSerializationSchema<CommentResult> {

        @Override
        public ProducerRecord<byte[], byte[]> serialize(CommentResult element, @Nullable Long timestamp) {
            try {
                // 将CommentResult对象转换为JSON字符串
                String json = JSON.toJSONString(element);

                // 创建ProducerRecord，key使用user_id，value为JSON字符串
                String key = element.getUser_id();  // 修正：使用getUser_id()而不是getUserid()
                return new ProducerRecord<>(
                        "realtime_v3_order_comment",
                        key != null ? key.getBytes(StandardCharsets.UTF_8) : null,
                        json.getBytes(StandardCharsets.UTF_8)
                );
            } catch (Exception e) {
                System.err.println("❌ 序列化CommentResult失败: " + e.getMessage());
                // 返回错误信息，避免数据丢失
                String errorJson = "{\"error\":\"serialization_failed\",\"message\":\"" + e.getMessage() + "\"}";
                return new ProducerRecord<>(
                        "realtime_v3_order_comment",
                        null,
                        errorJson.getBytes(StandardCharsets.UTF_8)
                );
            }
        }
    }

    /**
     * 测试分词器效果
     */
    private static void testIKAnalyzer() {
        System.out.println("=== IK分词器测试 ===");

        String testText = "这款夹克质量一般，收纳设计共铲党也不方便，性价比不高。";
        System.out.println("测试文本: " + testText);

        // 测试分词效果
        List<String> segments = IKAnalyzerUtil.smartSegments(testText);
        System.out.println("分词结果: " + segments);

        // 测试敏感词检测
        List<String> testP0Words = Arrays.asList("共铲党", "测试词");
        List<String> testP1Words = Arrays.asList("质量一般", "不方便");

        Map<String, List<String>> result = IKAnalyzerUtil.detectSensitiveWordsWithLevel(
                testText, testP0Words, testP1Words);

        System.out.println("P0匹配: " + result.get("P0"));
        System.out.println("P1匹配: " + result.get("P1"));
        System.out.println("=== 测试结束 ===\n");
    }

    /**
     * 敏感词类，包含词语和级别信息
     */
    public static class SensitiveWord {
        private String word;
        private String level;  // P0 或 P1

        public SensitiveWord() {
            // 默认构造函数用于序列化
        }

        public SensitiveWord(String word, String level) {
            this.word = word;
            this.level = level;
        }

        public String getWord() { return word; }
        public void setWord(String word) { this.word = word; }

        public String getLevel() { return level; }
        public void setLevel(String level) { this.level = level; }

        @Override
        public String toString() {
            return "SensitiveWord{word='" + word + "', level='" + level + "'}";
        }
    }

    /**
     * 评论结果类 - 保持与原来Doris表相同的结构
     */
    public static class CommentResult {
        private String user_id;
        private String orderid;
        private String username;
        private String comment;
        private int isBlack;
        private String commentLevel;
        private List<String> sensitiveWords;

        public CommentResult() {}

        public CommentResult(String user_id, String orderid, String username, String comment,
                             int isBlack, String commentLevel, List<String> sensitiveWords) {
            this.user_id = user_id;
            this.orderid = orderid;
            this.username = username;
            this.comment = comment;
            this.isBlack = isBlack;
            this.commentLevel = commentLevel;
            this.sensitiveWords = sensitiveWords;
        }

        // 修正：getter方法名与字段名保持一致
        public String getUser_id() { return user_id; }
        public void setUser_id(String user_id) { this.user_id = user_id; }

        public String getOrderid() { return orderid; }
        public void setOrderid(String orderid) { this.orderid = orderid; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getComment() { return comment; }
        public void setComment(String comment) { this.comment = comment; }

        public int getIsBlack() { return isBlack; }
        public void setIsBlack(int isBlack) { this.isBlack = isBlack; }

        public String getCommentLevel() { return commentLevel; }
        public void setCommentLevel(String commentLevel) { this.commentLevel = commentLevel; }

        public List<String> getSensitiveWords() { return sensitiveWords; }
        public void setSensitiveWords(List<String> sensitiveWords) { this.sensitiveWords = sensitiveWords; }

        @Override
        public String toString() {
            return "CommentResult{" +
                    "user_id='" + user_id + '\'' +
                    ", orderid='" + orderid + '\'' +
                    ", username='" + username + '\'' +
                    ", comment='" + comment + '\'' +
                    ", isBlack=" + isBlack +
                    ", commentLevel='" + commentLevel + '\'' +
                    ", sensitiveWords=" + sensitiveWords +
                    '}';
        }
    }

    /**
     * 评论分级处理类 - 保持原有逻辑不变
     */
    public static class CommentLevelProcessor extends KeyedBroadcastProcessFunction<String, String, SensitiveWord, CommentResult> {

        private MapStateDescriptor<String, SensitiveWord> sensitiveStateDescriptor =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        @Override
        public void processElement(String commentJson, ReadOnlyContext ctx, Collector<CommentResult> out) throws Exception {
            try {
                JSONObject obj = JSON.parseObject(commentJson);
                JSONObject after = obj.getJSONObject("after");
                if (after == null) {
                    return;
                }

                // 提取评论内容
                String commentText = extractCommentText(after.getString("comment"));
                if (commentText == null) {
                    commentText = "";
                }

                // 评论分级检测（使用优化的检测方法）
                CommentLevelResult levelResult = detectCommentLevelOptimized(commentText, ctx);

                // 构建输出结果
                CommentResult result = buildCommentResult(after, commentText, levelResult);
                out.collect(result);

                // 打印分级结果
                if (!"P2".equals(levelResult.getLevel())) {
                    System.out.println("📊 评论分级结果: " + levelResult.getLevel() +
                            ", 敏感词: " + levelResult.getSensitiveWords() +
                            ", 用户: " + after.getString("user_id"));
                }

            } catch (Exception e) {
                System.err.println("❌ 处理数据失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * 使用纯分词匹配的敏感词检测方法
         */
        private CommentLevelResult detectCommentLevelOptimized(String comment, ReadOnlyContext ctx) throws Exception {
            List<String> allMatchedWords = new ArrayList<>();
            String commentLevel = "P2";
            boolean hasP0Word = false;
            boolean hasP1Word = false;

            ReadOnlyBroadcastState<String, SensitiveWord> broadcastState =
                    ctx.getBroadcastState(sensitiveStateDescriptor);

            // 提取P0和P1敏感词
            List<String> p0Words = new ArrayList<>();
            List<String> p1Words = new ArrayList<>();

            for (Map.Entry<String, SensitiveWord> entry : broadcastState.immutableEntries()) {
                SensitiveWord sensitiveWord = entry.getValue();
                if ("P0".equals(sensitiveWord.getLevel())) {
                    p0Words.add(sensitiveWord.getWord());
                } else if ("P1".equals(sensitiveWord.getLevel())) {
                    p1Words.add(sensitiveWord.getWord());
                }
            }

            // 使用纯分词匹配
            Map<String, List<String>> detectionResult =
                    IKAnalyzerUtil.detectSensitiveWordsWithLevel(comment, p0Words, p1Words);

            List<String> matchedP0Words = detectionResult.get("P0");
            List<String> matchedP1Words = detectionResult.get("P1");

            // 记录匹配的敏感词
            if (matchedP0Words != null && !matchedP0Words.isEmpty()) {
                hasP0Word = true;
                allMatchedWords.addAll(matchedP0Words);
            } else if (matchedP1Words != null && !matchedP1Words.isEmpty()) {
                hasP1Word = true;
                allMatchedWords.addAll(matchedP1Words);
            }

            // 调试信息
            if (!allMatchedWords.isEmpty()) {
                System.out.println("🎯 分词匹配结果:");
                System.out.println("   评论: " + (comment.length() > 50 ? comment.substring(0, 50) + "..." : comment));
                System.out.println("   匹配的敏感词: " + allMatchedWords);
            }

            // 确定评论级别
            if (hasP0Word) {
                commentLevel = "P0";
            } else if (hasP1Word) {
                commentLevel = "P1";
            }

            return new CommentLevelResult(commentLevel, allMatchedWords);
        }

        /**
         * 构建评论结果
         */
        private CommentResult buildCommentResult(JSONObject after, String commentText, CommentLevelResult levelResult) {
            String username = after.getString("user_name");
            if (username == null) {
                username = "unknown_user";
            }

            return new CommentResult(
                    after.getString("user_id"),      // user_id
                    after.getString("order_id"),     // orderid
                    username,                        // username
                    commentText,                     // comment
                    levelResult.getLevel().equals("P0") ? 1 : 0,  // is_black
                    levelResult.getLevel(),          // comment_level
                    levelResult.getSensitiveWords()  // sensitive_words
            );
        }

        private String extractCommentText(String commentField) {
            if (commentField == null || commentField.trim().isEmpty()) {
                return "";
            }
            try {
                JSONObject commentJson = JSON.parseObject(commentField);
                if (commentJson.containsKey("comment")) {
                    return commentJson.getString("comment");
                }
                return commentField;
            } catch (Exception e) {
                return commentField;
            }
        }

        @Override
        public void processBroadcastElement(SensitiveWord sensitiveWord, Context ctx, Collector<CommentResult> out) throws Exception {
            ctx.getBroadcastState(sensitiveStateDescriptor).put(sensitiveWord.getWord(), sensitiveWord);
        }
    }

    /**
     * 评论分级结果类
     */
    public static class CommentLevelResult {
        private String level;  // P0, P1, P2
        private List<String> sensitiveWords; // 匹配的敏感词列表

        public CommentLevelResult(String level, List<String> sensitiveWords) {
            this.level = level;
            this.sensitiveWords = sensitiveWords;
        }

        public String getLevel() { return level; }
        public List<String> getSensitiveWords() { return sensitiveWords; }
    }
}