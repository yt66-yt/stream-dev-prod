package com.stream.realtime.lululemon.flinkcomment.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * IK 分词器工具类（稳定版 - 无兼容性问题）
 */
public class IKAnalyzerUtil {

    // 定时任务调度器
    private static final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    // 记录词典文件最后修改时间
    private static volatile long lastModified = 0;

    // 词典文件路径
    private static final String DICT_FILE_PATH = "custom/mydict.dic";

    // 是否启用热更新
    private static volatile boolean hotReloadEnabled = true;

    // 当前内存中的敏感词集合（用于快速匹配）
    private static final Set<String> memorySensitiveWords = new HashSet<>();

    // 静态初始化块 - 启动时执行
    static {
        try {
            System.out.println("✅ IK分词器工具类初始化开始");

            // 初始化最后修改时间
            initLastModifiedTime();

            // 加载内存敏感词
            loadMemorySensitiveWords();

            // 启动定时检查词典文件变化
            startDictFileWatcher();

            System.out.println("✅ IK分词器工具类初始化完成");

        } catch (Exception e) {
            System.err.println("❌ IK分词器工具类初始化失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 启动词典文件监控
     */
    private static void startDictFileWatcher() {
        if (!hotReloadEnabled) {
            System.out.println("⚠️ 词典热更新功能已禁用");
            return;
        }

        // 每30秒检查一次词典文件变化
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndReloadDict();
            } catch (Exception e) {
                System.err.println("❌ 词典文件监控异常: " + e.getMessage());
            }
        }, 60, 30, TimeUnit.SECONDS); // 启动后60秒开始，每30秒检查一次

        System.out.println("🔄 词典文件监控已启动，检查间隔: 30秒");
    }

    /**
     * 检查并重新加载词典
     */
    private static void checkAndReloadDict() {
        if (!hotReloadEnabled) {
            return;
        }

        try {
            // 获取词典文件
            java.net.URL dictUrl = IKAnalyzerUtil.class.getClassLoader().getResource(DICT_FILE_PATH);
            if (dictUrl == null) {
                System.err.println("❌ 找不到词典文件: " + DICT_FILE_PATH);
                return;
            }

            java.nio.file.Path dictPath;
            try {
                dictPath = java.nio.file.Paths.get(dictUrl.toURI());
            } catch (Exception e) {
                // 如果无法通过URI获取路径，尝试其他方式
                String filePath = dictUrl.getFile();
                // 处理Windows路径中的特殊字符
                if (filePath.contains("!")) {
                    // 在JAR包内的情况
                    System.out.println("📦 词典文件在JAR包内，无法热更新");
                    return;
                }
                dictPath = java.nio.file.Paths.get(filePath);
            }

            if (!java.nio.file.Files.exists(dictPath)) {
                System.err.println("❌ 词典文件不存在: " + dictPath);
                return;
            }

            long currentModified = java.nio.file.Files.getLastModifiedTime(dictPath).toMillis();

            if (currentModified > lastModified) {
                System.out.println("🔄 检测到词典文件变化，重新加载词典...");
                System.out.println("   文件: " + DICT_FILE_PATH);
                System.out.println("   上次修改: " + new Date(lastModified));
                System.out.println("   当前修改: " + new Date(currentModified));

                // 重新加载内存中的敏感词
                loadMemorySensitiveWords();
                lastModified = currentModified;

                System.out.println("✅ 词典重新加载完成");
                System.out.println("   内存敏感词数量: " + memorySensitiveWords.size());
                System.out.println("   当前时间: " + new Date());

                // 测试重新加载后的效果
                testDictReload();
            }

        } catch (Exception e) {
            System.err.println("❌ 词典重新加载失败: " + e.getMessage());
        }
    }

    /**
     * 加载内存敏感词
     */
    private static void loadMemorySensitiveWords() {
        try {
            java.net.URL dictUrl = IKAnalyzerUtil.class.getClassLoader().getResource(DICT_FILE_PATH);
            if (dictUrl == null) {
                System.err.println("❌ 找不到词典文件: " + DICT_FILE_PATH);
                return;
            }

            // 读取词典文件内容
            List<String> lines;
            try {
                lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(dictUrl.toURI()));
            } catch (Exception e) {
                // 如果无法通过URI读取，尝试其他方式
                String filePath = dictUrl.getFile();
                if (filePath.contains("!")) {
                    // 在JAR包内的情况，使用流读取
                    try (java.io.BufferedReader reader = new java.io.BufferedReader(
                            new java.io.InputStreamReader(dictUrl.openStream()))) {
                        lines = new ArrayList<>();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            lines.add(line);
                        }
                    }
                } else {
                    lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(filePath));
                }
            }

            // 清空并重新加载内存敏感词
            memorySensitiveWords.clear();
            for (String line : lines) {
                String word = line.trim();
                if (!word.isEmpty() && !word.startsWith("#")) { // 跳过空行和注释
                    memorySensitiveWords.add(word);
                }
            }

            System.out.println("✅ 内存敏感词加载完成，数量: " + memorySensitiveWords.size());

        } catch (Exception e) {
            System.err.println("❌ 加载内存敏感词失败: " + e.getMessage());
        }
    }

    /**
     * 测试词典重新加载效果
     */
    private static void testDictReload() {
        try {
            String testText = "共铲党测试文本";
            List<String> segments = smartSegments(testText);

            // 检查分词结果和内存敏感词
            boolean foundInSegments = segments.contains("共铲党");
            boolean foundInMemory = memorySensitiveWords.contains("共铲党");

            System.out.println("🔍 重新加载测试结果:");
            System.out.println("   分词结果: " + segments);
            System.out.println("   分词包含'共铲党': " + foundInSegments);
            System.out.println("   内存包含'共铲党': " + foundInMemory);

            if (foundInSegments && foundInMemory) {
                System.out.println("✅ 词典重新加载测试: 完全成功");
            } else if (foundInMemory) {
                System.out.println("⚠️ 词典重新加载测试: 内存加载成功，但分词器可能需要重启");
            } else {
                System.out.println("❌ 词典重新加载测试: 失败");
            }

        } catch (Exception e) {
            System.err.println("❌ 词典重新加载测试失败: " + e.getMessage());
        }
    }

    /**
     * 初始化最后修改时间
     */
    private static void initLastModifiedTime() {
        try {
            java.net.URL dictUrl = IKAnalyzerUtil.class.getClassLoader().getResource(DICT_FILE_PATH);
            if (dictUrl != null) {
                java.nio.file.Path dictPath;
                try {
                    dictPath = java.nio.file.Paths.get(dictUrl.toURI());
                } catch (Exception e) {
                    String filePath = dictUrl.getFile();
                    if (filePath.contains("!")) {
                        // 在JAR包内的情况，无法获取修改时间
                        System.out.println("📦 词典文件在JAR包内，使用当前时间");
                        lastModified = System.currentTimeMillis();
                        return;
                    }
                    dictPath = java.nio.file.Paths.get(filePath);
                }

                if (java.nio.file.Files.exists(dictPath)) {
                    lastModified = java.nio.file.Files.getLastModifiedTime(dictPath).toMillis();
                    System.out.println("📝 词典文件最后修改时间: " + new Date(lastModified));
                    System.out.println("📁 词典文件路径: " + dictPath);
                } else {
                    System.err.println("❌ 词典文件不存在: " + DICT_FILE_PATH);
                }
            } else {
                System.err.println("❌ 找不到词典文件: " + DICT_FILE_PATH);
            }
        } catch (Exception e) {
            System.err.println("❌ 获取词典文件信息失败: " + e.getMessage());
        }
    }

    /**
     * 预加载敏感词
     */
    public static void preloadSensitiveWords(List<String> sensitiveWords) {
        System.out.println("✅ 预加载敏感词数量: " + sensitiveWords.size());

        // 显示敏感词示例
        List<String> sampleWords = sensitiveWords.subList(0, Math.min(10, sensitiveWords.size()));
        System.out.println("📋 敏感词示例: " + sampleWords);

        // 同时添加到内存集合中（作为备份）
        memorySensitiveWords.addAll(sensitiveWords);
        System.out.println("✅ 内存敏感词总数: " + memorySensitiveWords.size());
    }

    /**
     * 手动触发词典重新加载
     */
    public static void manualReloadDict() {
        System.out.println("🔄 手动触发词典重新加载...");
        try {
            loadMemorySensitiveWords();
            initLastModifiedTime();
            System.out.println("✅ 手动重新加载完成");

            // 测试重新加载效果
            testDictReload();

        } catch (Exception e) {
            System.err.println("❌ 手动重新加载失败: " + e.getMessage());
        }
    }

    /**
     * 启用或禁用热更新功能
     */
    public static void setHotReloadEnabled(boolean enabled) {
        hotReloadEnabled = enabled;
        if (enabled) {
            System.out.println("✅ 词典热更新功能已启用");
        } else {
            System.out.println("⏸️ 词典热更新功能已禁用");
        }
    }

    /**
     * 获取热更新状态
     */
    public static boolean isHotReloadEnabled() {
        return hotReloadEnabled;
    }

    /**
     * 获取词典文件信息
     */
    public static void printDictFileInfo() {
        try {
            java.net.URL dictUrl = IKAnalyzerUtil.class.getClassLoader().getResource(DICT_FILE_PATH);
            if (dictUrl != null) {
                System.out.println("📁 词典文件信息:");
                System.out.println("   路径: " + DICT_FILE_PATH);
                System.out.println("   URL: " + dictUrl);
                System.out.println("   最后修改: " + new Date(lastModified));
                System.out.println("   热更新: " + (hotReloadEnabled ? "启用" : "禁用"));
                System.out.println("   内存敏感词数量: " + memorySensitiveWords.size());
            } else {
                System.err.println("❌ 找不到词典文件: " + DICT_FILE_PATH);
            }
        } catch (Exception e) {
            System.err.println("❌ 获取词典文件信息失败: " + e.getMessage());
        }
    }

    /**
     * 纯分词匹配的敏感词检测
     */
    public static Map<String, List<String>> detectSensitiveWordsWithLevel(String text,
                                                                          List<String> p0Words,
                                                                          List<String> p1Words) {
        Map<String, List<String>> result = new HashMap<>();
        List<String> p0Matched = new ArrayList<>();
        List<String> p1Matched = new ArrayList<>();

        if (text == null || text.trim().isEmpty()) {
            result.put("P0", p0Matched);
            result.put("P1", p1Matched);
            return result;
        }

        // 只使用分词器进行分词
        List<String> segments = smartSegments(text);

        System.out.println("🔍 分词结果: " + segments);

        // 检测P0敏感词 - 只检查分词结果中是否包含敏感词
        for (String word : p0Words) {
            if (segments.contains(word)) {
                p0Matched.add(word);
                System.out.println("🎯 匹配到P0敏感词: " + word);
                break; // P0只需要匹配一个词
            }
        }

        // 如果没匹配到P0，再匹配P1
        if (p0Matched.isEmpty()) {
            for (String word : p1Words) {
                if (segments.contains(word)) {
                    p1Matched.add(word);
                    System.out.println("🎯 匹配到P1敏感词: " + word);
                }
            }
        }

        result.put("P0", p0Matched);
        result.put("P1", p1Matched);
        return result;
    }

    /**
     * 增强版敏感词检测（分词 + 内存匹配）
     */
    public static Map<String, List<String>> detectSensitiveWordsEnhanced(String text,
                                                                         List<String> p0Words,
                                                                         List<String> p1Words) {
        Map<String, List<String>> result = new HashMap<>();
        List<String> p0Matched = new ArrayList<>();
        List<String> p1Matched = new ArrayList<>();

        if (text == null || text.trim().isEmpty()) {
            result.put("P0", p0Matched);
            result.put("P1", p1Matched);
            return result;
        }

        // 方法1: 使用分词器进行分词匹配
        List<String> segments = smartSegments(text);
        System.out.println("🔍 分词结果: " + segments);

        // 检测P0敏感词
        for (String word : p0Words) {
            if (segments.contains(word) || memorySensitiveWords.contains(word)) {
                p0Matched.add(word);
                System.out.println("🎯 匹配到P0敏感词: " + word);
                break;
            }
        }

        // 如果没匹配到P0，再匹配P1
        if (p0Matched.isEmpty()) {
            for (String word : p1Words) {
                if (segments.contains(word) || memorySensitiveWords.contains(word)) {
                    p1Matched.add(word);
                    System.out.println("🎯 匹配到P1敏感词: " + word);
                }
            }
        }

        result.put("P0", p0Matched);
        result.put("P1", p1Matched);
        return result;
    }

    /**
     * 对文本进行智能分词
     */
    public static List<String> smartSegments(String text) {
        List<String> result = new ArrayList<>();
        if (text == null || text.trim().isEmpty()) {
            return result;
        }

        try {
            StringReader reader = new StringReader(text.trim());
            IKSegmenter ikSegmenter = new IKSegmenter(reader, true); // 使用智能分词
            Lexeme lexeme;

            while ((lexeme = ikSegmenter.next()) != null) {
                String word = lexeme.getLexemeText();
                if (word != null && !word.trim().isEmpty()) {
                    result.add(word.trim());
                }
            }
        } catch (IOException e) {
            System.err.println("IK分词失败: " + e.getMessage());
            result.addAll(splitBySpace(text));
        }

        return result;
    }

    /**
     * 简单的空格分词（备用方案）
     */
    private static List<String> splitBySpace(String text) {
        List<String> result = new ArrayList<>();
        if (text == null) {
            return result;
        }

        String[] words = text.split("\\s+");
        for (String word : words) {
            if (!word.trim().isEmpty()) {
                result.add(word.trim());
            }
        }
        return result;
    }

    /**
     * 获取内存中的敏感词数量
     */
    public static int getMemorySensitiveWordsCount() {
        return memorySensitiveWords.size();
    }

    /**
     * 检查某个词是否在内存敏感词中
     */
    public static boolean containsInMemory(String word) {
        return memorySensitiveWords.contains(word);
    }

    /**
     * 关闭资源
     */
    public static void shutdown() {
        System.out.println("🛑 关闭词典监控服务...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("✅ 词典监控服务已关闭");
    }
}