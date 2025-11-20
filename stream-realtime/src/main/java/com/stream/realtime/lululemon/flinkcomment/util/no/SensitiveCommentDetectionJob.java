package com.stream.realtime.lululemon.flinkcomment.util.no;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SensitiveCommentDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置参数
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        EnvironmentSettingUtils.defaultParameter(env);

        // Kafka 配置
        String kafkaBootstrap = "172.24.158.53:9092";
        String kafkaTopic = "realtime_v3_order_comment";
        KafkaUtils.createKafkaTopic(kafkaBootstrap, kafkaTopic, 3, (short) 1, true);

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

        // 读取 P0 敏感词文件（高危敏感词）
        String p0SensitiveWordFile = "D:\\idealwj\\stream-dev-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\flinkcomment\\suspected-sensitive-words.txt";
        List<String> p0SensitiveWords = Files.lines(Paths.get(p0SensitiveWordFile))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());

        // 读取 P1 敏感词文件（脏话、地域歧视等）
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

        // 创建包含级别信息的敏感词列表
        List<SensitiveWord> allSensitiveWords = new ArrayList<>();

        // 添加 P0 敏感词
        for (String word : p0SensitiveWords) {
            allSensitiveWords.add(new SensitiveWord(word, "P0"));
        }

        // 添加 P1 敏感词
        for (String word : p1SensitiveWords) {
            allSensitiveWords.add(new SensitiveWord(word, "P1"));
        }

        MapStateDescriptor<String, SensitiveWord> sensitiveStateDescriptor =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        // 创建一个广播流包含所有敏感词
        BroadcastStream<SensitiveWord> broadcastWords = env.fromCollection(allSensitiveWords)
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
        SingleOutputStreamOperator<String> processedStream = keyedStream
                .connect(broadcastWords)
                .process(new CommentLevelProcessor())
                .name("comment-level-processor");

        // 写入 Kafka
        KafkaSink<String> kafkaSink = KafkaUtils.buildKafkaSink(kafkaBootstrap, kafkaTopic);
        processedStream.sinkTo(kafkaSink);

        env.execute("SQLServer Comment to Kafka with Level Classification");
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
     * 评论分级处理类
     */
    /**
     * 评论分级处理类
     */
    public static class CommentLevelProcessor extends KeyedBroadcastProcessFunction<String, String, SensitiveWord, String> {

        private MapStateDescriptor<String, SensitiveWord> sensitiveStateDescriptor =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        @Override
        public void processElement(String commentJson, ReadOnlyContext ctx, Collector<String> out) throws Exception {
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

                // 评论分级检测和敏感词替换
                CommentLevelResult levelResult = detectAndProcessComment(commentText, ctx);

                // 构建输出结果
                JSONObject result = buildOutputResult(after, commentText, levelResult);
                out.collect(result.toJSONString());

                // 打印分级结果（用于调试）
                System.out.println("📊 评论分级结果: " + levelResult.getLevel() +
                        ", 敏感词: " + levelResult.getSensitiveWords() +
                        ", 用户: " + after.getString("user_id"));

            } catch (Exception e) {
                System.err.println("❌ 处理数据失败: " + e.getMessage());
                System.err.println("原始数据: " + commentJson);
                e.printStackTrace();
            }
        }

        /**
         * 评论分级检测和敏感词替换
         */
        /**
         * 评论分级检测和敏感词替换
         */
        private CommentLevelResult detectAndProcessComment(String comment, ReadOnlyContext ctx) throws Exception {
            List<String> allMatchedWords = new ArrayList<>();
            String commentLevel = "P2"; // 默认为 P2
            boolean hasP0Word = false;
            boolean hasP1Word = false;

            // 用于敏感词替换的文本
            String processedComment = comment;

            ReadOnlyBroadcastState<String, SensitiveWord> broadcastState =
                    ctx.getBroadcastState(sensitiveStateDescriptor);

            // 第一步：先检测所有敏感词，不进行替换
            List<SensitiveWord> matchedSensitiveWords = new ArrayList<>();
            for (Map.Entry<String, SensitiveWord> entry : broadcastState.immutableEntries()) {
                SensitiveWord sensitiveWord = entry.getValue();
                String word = sensitiveWord.getWord();

                if (comment.contains(word)) {
                    matchedSensitiveWords.add(sensitiveWord);
                    allMatchedWords.add(word + "(" + sensitiveWord.getLevel() + ")");

                    if ("P0".equals(sensitiveWord.getLevel())) {
                        hasP0Word = true;
                    } else if ("P1".equals(sensitiveWord.getLevel())) {
                        hasP1Word = true;
                    }
                }
            }

            // 第二步：按词语长度降序排序（先替换长的词语，避免短词破坏长词）
            matchedSensitiveWords.sort((a, b) -> Integer.compare(b.getWord().length(), a.getWord().length()));

            // 第三步：一次性替换所有敏感词
            for (SensitiveWord sensitiveWord : matchedSensitiveWords) {
                String word = sensitiveWord.getWord();
                processedComment = processedComment.replace(word, "****");
            }

            // 确定评论级别（P0优先级最高）
            if (hasP0Word) {
                commentLevel = "P0";
            } else if (hasP1Word) {
                commentLevel = "P1";
            }

            return new CommentLevelResult(commentLevel, allMatchedWords, processedComment);
        }

        /**
         * 构建输出结果
         */
        private JSONObject buildOutputResult(JSONObject after, String commentText, CommentLevelResult levelResult) {
            JSONObject result = new JSONObject();

            // 复制所有原始字段
            for (Map.Entry<String, Object> entry : after.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // 特殊处理 comment 字段
                if ("comment".equals(key) && value instanceof String) {
                    String actualComment = extractCommentText((String) value);
                    result.put(key, actualComment != null ? actualComment : value);
                } else {
                    result.put(key, value);
                }
            }

            // 添加分级结果字段
            result.put("original_comment", commentText);
            result.put("processed_comment", levelResult.getProcessedComment());  // 使用替换后的文本
            result.put("sensitive_words", levelResult.getSensitiveWords());
            result.put("comment_level", levelResult.getLevel());
            result.put("is_black", levelResult.getLevel().equals("P0") ? 1 : 0); // P0 自动拉黑
            result.put("process_time", System.currentTimeMillis());

            return result;
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
        public void processBroadcastElement(SensitiveWord sensitiveWord, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(sensitiveStateDescriptor).put(sensitiveWord.getWord(), sensitiveWord);
        }
    }

    /**
     * 评论分级结果类
     */
    public static class CommentLevelResult {
        private String level;  // P0, P1, P2
        private List<String> sensitiveWords; // 匹配的敏感词列表
        private String processedComment; // 敏感词替换后的文本

        public CommentLevelResult(String level, List<String> sensitiveWords, String processedComment) {
            this.level = level;
            this.sensitiveWords = sensitiveWords;
            this.processedComment = processedComment;
        }

        public String getLevel() { return level; }
        public List<String> getSensitiveWords() { return sensitiveWords; }
        public String getProcessedComment() { return processedComment; }
    }
}