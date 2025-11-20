package com.stream.realtime.lululemon.flinkcomment;

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
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Properties;

public class CommentAnalysisDorisIK {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置参数
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        EnvironmentSettingUtils.defaultParameter(env);

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

        // 写入 Doris
        processedStream.addSink(new DorisCommentSink())
                .name("doris-comment-sink");

        env.execute("SQLServer Comment to Doris with Level Classification");
    }

    /**
     * 测试IK分词器效果
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
     * 评论结果类 - 对应Doris表结构
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

        // Getters and Setters
        public String getUserid() { return user_id; }
        public void setUserid(String user_id) { this.user_id = user_id; }

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
     * 评论分级处理类
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
            if (!matchedP0Words.isEmpty()) {
                hasP0Word = true;
                allMatchedWords.addAll(matchedP0Words);
            } else if (!matchedP1Words.isEmpty()) {
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

    /**
     * Doris Sink 实现 - JDBC 方式
     */
    public static class DorisCommentSink implements org.apache.flink.streaming.api.functions.sink.SinkFunction<CommentResult> {

        private transient Connection connection;
        private transient PreparedStatement preparedStatement;

        // Doris 连接配置
        private final String jdbcUrl = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
        private final String username = "root";
        private final String password = "";

        // 连接超时和重试机制
        private static final int MAX_RETRIES = 3;
        private static final long RETRY_INTERVAL = 5000; // 5秒

        @Override
        public void invoke(CommentResult value, Context context) throws Exception {
            int retries = 0;
            boolean success = false;

            while (retries < MAX_RETRIES && !success) {
                try {
                    if (connection == null || connection.isClosed()) {
                        initializeConnection();
                    }

                    // 设置参数
                    preparedStatement.setString(1, value.getUserid());
                    preparedStatement.setString(2, value.getOrderid());
                    preparedStatement.setString(3, value.getUsername());
                    preparedStatement.setString(4, value.getComment());
                    preparedStatement.setInt(5, value.getIsBlack());
                    preparedStatement.setString(6, value.getCommentLevel());
                    // 将敏感词列表转换为JSON字符串存储
                    String sensitiveWordsJson = JSON.toJSONString(value.getSensitiveWords());
                    preparedStatement.setString(7, sensitiveWordsJson);

                    // 执行插入
                    preparedStatement.executeUpdate();

                    System.out.println("✅ 成功写入Doris: " + value.getUserid() + " - " + value.getOrderid());
                    success = true;

                } catch (SQLException e) {
                    retries++;
                    System.err.println("❌ 写入Doris失败 (重试 " + retries + "/" + MAX_RETRIES + "): " + e.getMessage());

                    if (retries < MAX_RETRIES) {
                        System.out.println("⏳ " + RETRY_INTERVAL/1000 + "秒后重试...");
                        Thread.sleep(RETRY_INTERVAL);
                        reconnect();
                    } else {
                        System.err.println("❌ 达到最大重试次数，跳过此条数据: " + value.toString());
                    }
                }
            }
        }

        private void initializeConnection() throws SQLException {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");

                // 添加连接参数
                Properties props = new Properties();
                props.setProperty("user", username);
                props.setProperty("password", password);
                props.setProperty("connectTimeout", "30000");
                props.setProperty("socketTimeout", "60000");
                props.setProperty("autoReconnect", "true");

                connection = DriverManager.getConnection(jdbcUrl, props);

                // 准备插入语句
                String sql = "INSERT INTO comment_analysis (user_id, orderid, username, comment, is_black, comment_level, sensitive_words) VALUES (?, ?, ?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(sql);

                System.out.println("✅ Doris 连接初始化成功: " + jdbcUrl);

            } catch (ClassNotFoundException e) {
                throw new SQLException("MySQL JDBC Driver not found", e);
            }
        }

        private void reconnect() throws SQLException {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            initializeConnection();
        }

        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}