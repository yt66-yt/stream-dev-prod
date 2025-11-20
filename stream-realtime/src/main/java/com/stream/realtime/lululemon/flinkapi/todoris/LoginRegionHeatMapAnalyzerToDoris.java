package com.stream.realtime.lululemon.flinkapi.todoris;

import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Flink IP转省份处理程序 - 增强版（包含运营商信息），输出地域热力图到Doris
 */
@Slf4j
public class LoginRegionHeatMapAnalyzerToDoris {

    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String KAFKA_LOG_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_CONSUMER_GROUP = "flink-ip-province-group";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // Doris 配置 - 参照 UserPathAnalysisTasktodoris
    private static final String DORIS_URL = "jdbc:mysql://192.168.200.30:9030/flinkapi_lululemon";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 增强的IP地理位置API列表，包含更多备用API
    private static final String[] IP_API_URLS = {
            "http://whois.pconline.com.cn/ipJson.jsp?ip=%s&json=true",  // 国内API优先，返回中文
            "http://ip-api.com/json/%s?fields=status,message,country,regionName,city,isp,query&lang=zh-CN",
            "https://ipapi.co/%s/json/",  // 备用API
            "http://ip.taobao.com/service/getIpInfo.php?ip=%s",  // 淘宝IP库
            "http://api.ip138.com/query/?ip=%s&datatype=jsonp",  // IP138 API
            "https://whois.pconline.com.cn/ipJson.jsp?ip=%s&json=true",  // 国内API优先
            "https://api.ip.sb/geoip/%s", // 国外API，支持全球
            "https://ip.useragentinfo.com/json?ip=%s", // 国内API
    };

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        log.info("=== 启动Flink IP转省份作业 - 增强IP定位版本（含运营商），输出地域热力图到Doris ===");

        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOOTSTRAP_SERVERS, KAFKA_LOG_TOPIC, KAFKA_CONSUMER_GROUP, OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "kafka_log_source"
        );

        SingleOutputStreamOperator<String> monitoredStream = kafkaSource
                .map(value -> {
                    log.debug("收到原始数据: {}", value.substring(0, Math.min(value.length(), 100)));
                    return value;
                })
                .name("raw-data-monitor");

        // 使用累加计数器
        SingleOutputStreamOperator<CountResult> cumulativeCountStream = monitoredStream
                .keyBy(value -> "constant-key")
                .process(new CumulativeCountFunction())
                .name("cumulative-counter");

        cumulativeCountStream.print("累计统计");

        SingleOutputStreamOperator<ProcessedLogEvent> resultStream = monitoredStream
                .map(new EnhancedIpToLocationMapper())
                .name("enhanced-ip-mapper");

        // 添加调试输出
        resultStream.map(event -> {
            log.info("处理结果 - IP: {}, 省份: {}, 城市: {}, 运营商: {}",
                    event.getIp(), event.getProvince(), event.getCity(), event.getIsp());
            return event;
        }).name("debug-output");

        resultStream.print().name("console-sink");

        // 修改地域热力图映射器，去掉district字段，并过滤ISP为未知的记录
        SingleOutputStreamOperator<RegionHeatMapRecord> regionHeatMapStream = resultStream
                .filter(event -> event.getProvince() != null && !"未知".equals(event.getProvince()))
                .filter(event -> event.getIsp() != null && !"未知".equals(event.getIsp())) // 新增过滤条件：ISP不为未知
                .map(new MapFunction<ProcessedLogEvent, RegionHeatMapRecord>() {
                    @Override
                    public RegionHeatMapRecord map(ProcessedLogEvent event) throws Exception {
                        // 构建region字段：省份|城市|运营商
                        String region = String.format("%s|%s|%s",
                                event.getProvince(),
                                event.getCity(),
                                event.getIsp());

                        String ptStr = event.getFormattedDate().substring(0, 10); // 取日期部分 yyyy-MM-dd
                        String province = event.getProvince();
                        String city = event.getCity();
                        String isp = event.getIsp();

                        // 去掉district参数
                        return new RegionHeatMapRecord(ptStr, region, province, city, isp, 1L);
                    }
                })
                .name("region-heatmap-mapper");

// 按地域分组统计
        SingleOutputStreamOperator<RegionHeatMapRecord> heatMapCountStream = regionHeatMapStream
                .keyBy(record -> record.getPt() + "|" + record.getRegion())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .reduce((record1, record2) -> {
                    record1.setCount(record1.getCount() + record2.getCount());
                    return record1;
                })
                .name("region-heatmap-counter");

// 修改JDBC Sink，去掉district字段，并确保只写入ISP不为未知的记录
        heatMapCountStream
                .filter(record -> record.getIsp() != null && !"未知".equals(record.getIsp())) // 再次确保过滤
                .addSink(JdbcSink.sink(
                        "INSERT INTO login_region_heatmap_simple " +
                                "(pt, region, province, city, isp, count) " +  // 去掉district字段
                                "VALUES (?, ?, ?, ?, ?, ?)",  // 参数减少为6个
                        (ps, record) -> {
                            ps.setDate(1, record.getPt());
                            ps.setString(2, record.getRegion());
                            ps.setString(3, record.getProvince());
                            ps.setString(4, record.getCity());
                            ps.setString(5, record.getIsp());  // isp字段位置前移
                            ps.setLong(6, record.getCount());  // count字段位置调整
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
                )).name("login_region_heatmap_to_doris");
        // 修改控制台输出，去掉district字段
        heatMapCountStream
                .map(record -> String.format("登录地域热力图: pt=%s, region=%s, province=%s, city=%s, isp=%s, count=%d",
                        record.getPt(), record.getRegion(), record.getProvince(),
                        record.getCity(), record.getIsp(), record.getCount()))
                .print("login-region-heatmap-output");

        // 窗口统计
        heatMapCountStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new ProcessAllWindowFunction<RegionHeatMapRecord, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<RegionHeatMapRecord> elements, Collector<String> out) throws Exception {
                        long totalRecords = 0L;
                        Set<String> regions = new HashSet<>();
                        Set<String> provinces = new HashSet<>();
                        Set<String> cities = new HashSet<>();
                        Set<String> isps = new HashSet<>();

                        for (RegionHeatMapRecord record : elements) {
                            totalRecords += record.getCount();
                            regions.add(record.getRegion());
                            provinces.add(record.getProvince());
                            cities.add(record.getCity());
                            isps.add(record.getIsp());
                        }

                        String stats = String.format(
                                "=== 登录地域热力图窗口统计 ===\n" +
                                        "窗口结束时间: %s\n" +
                                        "唯一区域数: %d\n" +
                                        "覆盖省份数: %d\n" +
                                        "覆盖城市数: %d\n" +
                                        "运营商数: %d\n" +
                                        "总登录次数: %d\n" +
                                        "==========================",
                                LocalDateTime.now().format(TIME_FORMATTER),
                                regions.size(),
                                provinces.size(),
                                cities.size(),
                                isps.size(),
                                totalRecords
                        );
                        out.collect(stats);
                    }
                })
                .print("heatmap-window-statistics");

        env.execute("Flink IP to Location Transformation - Enhanced IP Location with ISP and Region HeatMap to Doris");
    }

    /**
     * 登录地域热力图记录实体类 - 对应Doris表login_region_heatmap
     */
    // 修改 RegionHeatMapRecord 类
    @Data
    public static class RegionHeatMapRecord {
        private java.sql.Date pt;     // Date 类型
        private String region;
        private String province;
        private String city;
        private String isp;
        private Long count;

        public RegionHeatMapRecord() {}

        public RegionHeatMapRecord(String ptStr, String region, String province, String city, String isp, Long count) {
            try {
                // 将字符串日期转换为java.sql.Date
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date utilDate = sdf.parse(ptStr);
                this.pt = new java.sql.Date(utilDate.getTime());
            } catch (Exception e) {
                // 如果解析失败，使用当前日期
                log.warn("日期解析失败: {}, 使用当前日期", ptStr);
                this.pt = new java.sql.Date(System.currentTimeMillis());
            }
            this.region = region;
            this.province = province;
            this.city = city;
            this.isp = isp;
            this.count = count;
        }
    }

    /**
     * 累加计数器 - 使用状态管理累加计数
     */
    public static class CumulativeCountFunction extends KeyedProcessFunction<String, String, CountResult> {
        private transient ValueState<Long> countState;
        private transient ValueState<Long> lastWindowEndState;
        private long windowSize = 30000; // 30秒窗口

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>("countState", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Long> lastWindowDescriptor = new ValueStateDescriptor<>("lastWindowEndState", Long.class);
            lastWindowEndState = getRuntimeContext().getState(lastWindowDescriptor);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<CountResult> out) throws Exception {
            // 获取当前处理时间
            long currentTime = ctx.timerService().currentProcessingTime();
            long windowEnd = (currentTime / windowSize) * windowSize + windowSize;

            // 获取当前计数
            Long currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0L;
            }

            // 递增计数
            currentCount++;
            countState.update(currentCount);

            // 检查是否需要触发窗口输出
            Long lastWindowEnd = lastWindowEndState.value();
            if (lastWindowEnd == null || windowEnd > lastWindowEnd) {
                // 注册定时器在窗口结束时触发
                ctx.timerService().registerProcessingTimeTimer(windowEnd);
                lastWindowEndState.update(windowEnd);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<CountResult> out) throws Exception {
            Long currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0L;
            }

            long windowStart = timestamp - windowSize;
            String windowInfo = String.format("%s - %s",
                    DATE_FORMAT.format(new Date(windowStart)),
                    DATE_FORMAT.format(new Date(timestamp)));

            out.collect(new CountResult(windowInfo, currentCount, new Date(timestamp)));
        }
    }

    /**
     * 计数结果类
     */
    @Data
    public static class CountResult {
        private String windowInfo;
        private Long count;
        private Date timestamp;

        public CountResult(String windowInfo, Long count, Date timestamp) {
            this.windowInfo = windowInfo;
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("窗口时间: %s | 累计处理消息数量: %d 条", windowInfo, count);
        }
    }

    /**
     * 增强的IP定位映射器
     */
    public static class EnhancedIpToLocationMapper extends RichMapFunction<String, ProcessedLogEvent> {
        private ConcurrentHashMap<String, String[]> ipCache;
        private long lastApiCallTime = 0;
        private static final long API_CALL_INTERVAL = 1200; // 1.2秒间隔，避免频繁调用

        // 记录无法解析的IP，避免重复尝试
        private ConcurrentHashMap<String, Long> failedIps;

        @Override
        public void open(Configuration parameters) throws Exception {
            ipCache = new ConcurrentHashMap<>();
            failedIps = new ConcurrentHashMap<>();
            log.info("增强IP地理位置查询器初始化完成");
        }

        @Override
        public ProcessedLogEvent map(String jsonStr) throws Exception {
            try {
                LogEvent logEvent = JSON.parseObject(jsonStr, LogEvent.class);
                String ip = extractIpAddress(logEvent);

                String province = "未知";
                String city = "未知";
                String isp = "未知";

                if (isValidIp(ip)) {
                    // 检查是否为已知无法解析的IP
                    if (failedIps.containsKey(ip)) {
                        log.debug("跳过已知无法解析的IP: {}", ip);
                    } else {
                        String[] location = getLocationFromIp(ip);
                        province = location[0];
                        city = location[1];
                        isp = location[2]; // 新增运营商信息
                        log.debug("IP {} 解析为: 省份={}, 城市={}, 运营商={}", ip, province, city, isp);
                    }
                } else {
                    log.warn("无效IP地址: {}", ip);
                }

                String formattedDate = formatTimestamp(logEvent.getTs());

                ProcessedLogEvent result = new ProcessedLogEvent();
                result.setLogId(logEvent.getLogId());
                result.setIp(ip != null ? ip : "无");
                result.setProvince(province);
                result.setCity(city);
                result.setIsp(isp); // 设置运营商
                result.setLogType(logEvent.getLogType());
                result.setUserId(logEvent.getUserId());
                result.setProductId(logEvent.getProductId());
                result.setOrderId(logEvent.getOrderId());
                result.setTimestamp(logEvent.getTs());
                result.setFormattedDate(formattedDate);

                if (logEvent.getDevice() != null) {
                    result.setBrand(logEvent.getDevice().getBrand());
                } else {
                    result.setBrand("未知");
                }

                if (logEvent.getNetwork() != null) {
                    result.setNetwork(logEvent.getNetwork().getNet());
                } else {
                    result.setNetwork("未知");
                }

                result.setOriginalData(jsonStr);

                return result;

            } catch (Exception e) {
                log.error("处理日志失败: {}, 原始数据: {}", e.getMessage(),
                        jsonStr.substring(0, Math.min(jsonStr.length(), 200)), e);

                ProcessedLogEvent errorEvent = new ProcessedLogEvent();
                errorEvent.setLogId("parse-error");
                errorEvent.setIp("解析错误");
                errorEvent.setProvince("解析错误");
                errorEvent.setCity("解析错误");
                errorEvent.setIsp("解析错误");
                errorEvent.setLogType("error");
                errorEvent.setFormattedDate(DATE_FORMAT.format(new Date()));
                return errorEvent;
            }
        }

        private String extractIpAddress(LogEvent logEvent) {
            if (logEvent == null) {
                return null;
            }

            // 多层尝试获取IP
            if (logEvent.getGis() != null && logEvent.getGis().getIp() != null) {
                String ip = logEvent.getGis().getIp().trim();
                if (!ip.isEmpty() && !"null".equalsIgnoreCase(ip)) {
                    return ip;
                }
            }

            return null;
        }

        private boolean isValidIp(String ip) {
            if (ip == null || ip.isEmpty() || "null".equalsIgnoreCase(ip)) {
                return false;
            }

            // 简单IP格式验证
            return ip.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
        }

        private String[] getLocationFromIp(String ip) {
            // 检查缓存
            if (ipCache.containsKey(ip)) {
                String[] cached = ipCache.get(ip);
                // 检查缓存结果是否有效
                if (isValidLocation(cached[0], cached[1])) {
                    log.debug("从缓存获取IP {} 的位置: {}/{} 运营商: {}", ip, cached[0], cached[1], cached[2]);
                    return cached;
                } else {
                    // 无效缓存，移除
                    ipCache.remove(ip);
                }
            }

            // 避免频繁调用API
            long currentTime = System.currentTimeMillis();
            long timeSinceLastCall = currentTime - lastApiCallTime;
            if (timeSinceLastCall < API_CALL_INTERVAL) {
                try {
                    long sleepTime = API_CALL_INTERVAL - timeSinceLastCall;
                    log.debug("API调用间隔保护，等待 {}ms", sleepTime);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new String[]{"未知", "未知", "未知"};
                }
            }

            String[] result = {"未知", "未知", "未知"};
            boolean locationFound = false;

            // 尝试多个API
            for (int i = 0; i < IP_API_URLS.length; i++) {
                String apiUrl = IP_API_URLS[i];
                try {
                    String url = String.format(apiUrl, ip);
                    log.debug("尝试API {}: {}", i + 1, url);

                    String response = httpGet(url, i);
                    log.debug("API {} 响应: {}", i + 1, response);

                    if (response != null && !response.isEmpty()) {
                        String[] location = parseApiResponse(response, i);
                        if (isValidLocation(location[0], location[1])) {
                            result = location;
                            locationFound = true;
                            log.info("API {} 解析成功: {} -> 省份: {}, 城市: {}, 运营商: {}",
                                    i + 1, ip, location[0], location[1], location[2]);
                            break;
                        } else {
                            log.debug("API {} 返回未知位置", i + 1);
                        }
                    } else {
                        log.debug("API {} 返回空响应", i + 1);
                    }
                } catch (Exception e) {
                    log.debug("API {} 调用失败 {}: {}", i + 1, ip, e.getMessage());
                }
            }

            // 如果所有API都无法解析，记录这个IP
            if (!locationFound) {
                log.warn("所有API都无法解析IP: {}", ip);
                failedIps.put(ip, System.currentTimeMillis());

                // 限制失败IP缓存大小
                if (failedIps.size() > 500) {
                    failedIps.clear();
                    log.info("清空失败IP缓存");
                }
            }

            // 如果运营商仍然是未知，尝试基于IP段猜测
            if ("未知".equals(result[2])) {
                String guessedISP = guessISPByIP(ip);
                if (!"未知".equals(guessedISP)) {
                    result[2] = guessedISP;
                    log.info("基于IP段猜测运营商: {} -> {}", ip, guessedISP);
                }
            }

            lastApiCallTime = System.currentTimeMillis();
            ipCache.put(ip, result);

            // 限制缓存大小
            if (ipCache.size() > 1000) {
                ipCache.clear();
                log.info("清空IP缓存");
            }

            return result;
        }

        private String httpGet(String urlStr, int apiIndex) throws Exception {
            HttpURLConnection conn = null;
            BufferedReader reader = null;
            try {
                URL url = new URL(urlStr);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(8000);
                conn.setReadTimeout(8000);
                conn.setRequestProperty("User-Agent", "Mozilla/5.0 (compatible; Flink-IP-Lookup/1.0)");
                conn.setRequestProperty("Accept", "application/json");

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    // 根据API索引决定编码
                    if (apiIndex == 0) {
                        // 太平洋网络API使用GBK编码
                        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "GBK"));
                    } else {
                        // 其他API使用UTF-8
                        reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    }

                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    return response.toString();
                } else {
                    log.debug("HTTP请求失败，状态码: {}", responseCode);
                }
            } catch (Exception e) {
                log.debug("HTTP请求异常: {}", e.getMessage());
                throw e;
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                if (conn != null) {
                    conn.disconnect();
                }
            }
            return null;
        }

        private String[] parseApiResponse(String response, int apiIndex) {
            String[] result = {"未知", "未知", "未知"};

            try {
                log.debug("API {} 原始响应: {}", apiIndex, response);

                switch (apiIndex) {
                    case 0: // whois.pconline.com.cn - 国内API，直接使用中文
                        if (response.contains("\"pro\"")) {
                            JSONObject pconlineResponse = JSON.parseObject(response);
                            if (pconlineResponse != null) {
                                result[0] = pconlineResponse.getString("pro") != null ?
                                        pconlineResponse.getString("pro") : "未知";
                                result[1] = pconlineResponse.getString("city") != null ?
                                        pconlineResponse.getString("city") : "未知";
                                result[2] = pconlineResponse.getString("addr") != null ?
                                        extractISPFromAddr(pconlineResponse.getString("addr")) : "未知";

                                // 处理可能的"省"字缺失
                                if (!result[0].endsWith("省") && !result[0].endsWith("市") &&
                                        !result[0].endsWith("自治区") && !"未知".equals(result[0])) {
                                    result[0] = result[0] + "省";
                                }

                                log.debug("API0解析 - 省份: {}, 城市: {}, 地址: {}, 运营商: {}",
                                        result[0], result[1], pconlineResponse.getString("addr"), result[2]);
                            }
                        }
                        break;
                    case 1: // ip-api.com
                        if (response.contains("\"status\":\"success\"")) {
                            IpApiResponse apiResponse = JSON.parseObject(response, IpApiResponse.class);
                            if ("success".equals(apiResponse.status)) {
                                result[0] = convertProvinceToChinese(apiResponse.regionName);
                                result[1] = convertCityToChinese(apiResponse.city);
                                result[2] = convertISPToChinese(apiResponse.isp);
                                log.debug("API1解析 - 原始ISP: {}, 转换后: {}", apiResponse.isp, result[2]);
                            }
                        }
                        break;
                    case 2: // ipapi.co
                        JSONObject jsonResponse = JSON.parseObject(response);
                        if (jsonResponse != null && !jsonResponse.containsKey("error")) {
                            String region = jsonResponse.getString("region");
                            String city = jsonResponse.getString("city");
                            String org = jsonResponse.getString("org");
                            result[0] = convertProvinceToChinese(region);
                            result[1] = convertCityToChinese(city);
                            result[2] = extractISPFromOrg(org);
                            log.debug("API2解析 - 原始ORG: {}, 运营商: {}", org, result[2]);
                        }
                        break;
                    case 3: // 淘宝IP库
                        if (response.contains("\"code\":0")) {
                            JSONObject taobaoResponse = JSON.parseObject(response);
                            if (taobaoResponse != null && taobaoResponse.getIntValue("code") == 0) {
                                JSONObject data = taobaoResponse.getJSONObject("data");
                                if (data != null) {
                                    result[0] = data.getString("region") != null ?
                                            data.getString("region") : "未知";
                                    result[1] = data.getString("city") != null ?
                                            data.getString("city") : "未知";
                                    result[2] = data.getString("isp") != null ?
                                            convertISPToChinese(data.getString("isp")) : "未知";
                                    log.debug("API3解析 - 原始ISP: {}, 转换后: {}", data.getString("isp"), result[2]);
                                }
                            }
                        }
                        break;
                    case 4: // IP138 API
                        if (response.contains("\"ret\":\"ok\"")) {
                            JSONObject ip138Response = JSON.parseObject(response);
                            if (ip138Response != null && "ok".equals(ip138Response.getString("ret"))) {
                                // IP138返回的是数组，第一个元素是国家，第二个是省份，第三个是城市
                                List<String> data = ip138Response.getList("data", String.class);
                                if (data != null && data.size() >= 4) {
                                    result[0] = data.get(1) != null ? data.get(1) : "未知";
                                    result[1] = data.get(2) != null ? data.get(2) : "未知";
                                    result[2] = data.get(3) != null ? convertISPToChinese(data.get(3)) : "未知";
                                    log.debug("API4解析 - 原始ISP: {}, 转换后: {}", data.get(3), result[2]);
                                }
                            }
                        }
                        break;
                    case 5: // api.ip.sb - 国外API
                        JSONObject ipSbJson = JSON.parseObject(response);
                        if (ipSbJson != null) {
                            String country = ipSbJson.getString("country");
                            String region = ipSbJson.getString("region");
                            String city = ipSbJson.getString("city");
                            String isp = ipSbJson.getString("isp");

                            if (country != null && !country.isEmpty()) {
                                // 国外地址
                                if (!"CN".equals(country) && !"China".equalsIgnoreCase(country)) {
                                    result[0] = country;
                                    result[1] = city != null ? city : region != null ? region : country;
                                    result[2] = isp != null ? isp : "未知";
                                } else {
                                    // 国内地址
                                    result[0] = convertProvinceToChinese(region);
                                    result[1] = convertCityToChinese(city);
                                    result[2] = convertISPToChinese(isp);
                                }
                            }
                            log.debug("API5解析 - 原始ISP: {}, 转换后: {}", isp, result[2]);
                        }
                        break;
                    default:
                        log.debug("未知API索引: {}", apiIndex);
                }

                // 验证结果，如果包含问号表示编码有问题
                if (containsEncodingIssue(result[0]) || containsEncodingIssue(result[1]) || containsEncodingIssue(result[2])) {
                    log.warn("检测到编码问题: 省份={}, 城市={}, 运营商={}", result[0], result[1], result[2]);
                    result[0] = "未知";
                    result[1] = "未知";
                    result[2] = "未知";
                }

                log.debug("API {} 最终解析结果: 省份={}, 城市={}, 运营商={}", apiIndex, result[0], result[1], result[2]);

            } catch (Exception e) {
                log.debug("API响应解析失败: {}", e.getMessage());
            }

            return result;
        }

        /**
         * 从地址信息中提取运营商 - 增强版
         */
        private String extractISPFromAddr(String addr) {
            if (addr == null || addr.isEmpty()) {
                return "未知";
            }

            log.debug("提取运营商从地址: {}", addr);

            // 主要运营商
            if (addr.contains("电信") || addr.contains("ChinaNet") || addr.contains("CHINANET")) {
                return "电信";
            }
            if (addr.contains("联通") || addr.contains("联通宽带") || addr.contains("中国联通") || addr.contains("UNICOM")) {
                return "联通";
            }
            if (addr.contains("移动") || addr.contains("中国移动") || addr.contains("CMCC") || addr.contains("CMNET")) {
                return "移动";
            }

            // 二级运营商
            if (addr.contains("铁通") || addr.contains("中国铁通") || addr.contains("CTT")) {
                return "铁通";
            }
            if (addr.contains("长城宽带") || addr.contains("长城") || addr.contains("GWBN") || addr.contains("GreatWall")) {
                return "长城宽带";
            }
            if (addr.contains("教育网") || addr.contains("CERNET") || addr.contains("校园网")) {
                return "教育网";
            }
            if (addr.contains("广电") || addr.contains("有线电视") || addr.contains("CABLE")) {
                return "广电";
            }
            if (addr.contains("鹏博士") || addr.contains("DrPeng") || addr.contains("DRPENG")) {
                return "鹏博士";
            }
            if (addr.contains("方正宽带") || addr.contains("FOUNDER")) {
                return "方正宽带";
            }
            if (addr.contains("科技网") || addr.contains("CSTNET")) {
                return "科技网";
            }

            // 云服务商
            if (addr.contains("阿里云") || addr.contains("Alibaba") || addr.contains("ALIYUN")) {
                return "阿里云";
            }
            if (addr.contains("腾讯云") || addr.contains("Tencent") || addr.contains("TENCENT")) {
                return "腾讯云";
            }
            if (addr.contains("华为云") || addr.contains("Huawei") || addr.contains("HUAWEI")) {
                return "华为云";
            }
            if (addr.contains("百度云") || addr.contains("Baidu") || addr.contains("BAIDU")) {
                return "百度云";
            }
            if (addr.contains("京东云") || addr.contains("JDCloud") || addr.contains("JDCLOUD")) {
                return "京东云";
            }
            if (addr.contains("金山云") || addr.contains("Kingsoft") || addr.contains("KSCLOUD")) {
                return "金山云";
            }
            if (addr.contains("UCloud") || addr.contains("UCLOUD")) {
                return "UCloud";
            }
            if (addr.contains("青云") || addr.contains("QingCloud") || addr.contains("QINGCLOUD")) {
                return "青云";
            }

            // 其他网络服务商
            if (addr.contains("世纪互联") || addr.contains("21Vianet")) {
                return "世纪互联";
            }
            if (addr.contains("光环新网") || addr.contains("SINNET")) {
                return "光环新网";
            }
            if (addr.contains("网宿科技") || addr.contains("Wangsu")) {
                return "网宿科技";
            }
            if (addr.contains("蓝汛") || addr.contains("ChinaCache")) {
                return "蓝汛";
            }

            // 国际运营商
            if (addr.contains("AT&T") || addr.contains("ATT")) {
                return "AT&T";
            }
            if (addr.contains("Verizon") || addr.contains("VERIZON")) {
                return "Verizon";
            }
            if (addr.contains("Comcast") || addr.contains("COMCAST")) {
                return "Comcast";
            }
            if (addr.contains("T-Mobile") || addr.contains("TMOBILE")) {
                return "T-Mobile";
            }
            if (addr.contains("Sprint") || addr.contains("SPRINT")) {
                return "Sprint";
            }
            if (addr.contains("Vodafone") || addr.contains("VODAFONE")) {
                return "Vodafone";
            }
            if (addr.contains("Orange") || addr.contains("ORANGE")) {
                return "Orange";
            }
            if (addr.contains("Deutsche Telekom") || addr.contains("DT")) {
                return "德国电信";
            }

            // 亚太地区运营商
            if (addr.contains("NTT") || addr.contains("Nippon")) {
                return "NTT";
            }
            if (addr.contains("KDDI") || addr.contains("KDDI")) {
                return "KDDI";
            }
            if (addr.contains("SoftBank") || addr.contains("SOFTBANK")) {
                return "SoftBank";
            }
            if (addr.contains("SK Telecom") || addr.contains("SKT")) {
                return "SK Telecom";
            }
            if (addr.contains("KT") || addr.contains("Korea Telecom")) {
                return "KT";
            }
            if (addr.contains("LG Uplus") || addr.contains("LGUPLUS")) {
                return "LG Uplus";
            }
            if (addr.contains("SingTel") || addr.contains("SINGTEL")) {
                return "SingTel";
            }
            if (addr.contains("StarHub") || addr.contains("STARHUB")) {
                return "StarHub";
            }

            return "未知";
        }

        /**
         * 从组织信息中提取运营商 - 增强版
         */
        private String extractISPFromOrg(String org) {
            if (org == null || org.isEmpty()) {
                return "未知";
            }

            log.debug("提取运营商从组织: {}", org);

            org = org.toLowerCase();

            // 中国主要运营商
            if (org.contains("china telecom") || org.contains("电信") || org.contains("chinanet")) {
                return "电信";
            }
            if (org.contains("china unicom") || org.contains("联通") || org.contains("cnc") || org.contains("unicom")) {
                return "联通";
            }
            if (org.contains("china mobile") || org.contains("移动") || org.contains("cmnet") || org.contains("cmcc")) {
                return "移动";
            }
            if (org.contains("china tietong") || org.contains("铁通") || org.contains("ctt") || org.contains("tietong")) {
                return "铁通";
            }

            // 中国二级运营商
            if (org.contains("cernet") || org.contains("教育网") || org.contains("edu")) {
                return "教育网";
            }
            if (org.contains("great wall") || org.contains("长城宽带") || org.contains("gwbn")) {
                return "长城宽带";
            }
            if (org.contains("cable") || org.contains("广电") || org.contains("有线电视")) {
                return "广电";
            }
            if (org.contains("dr.peng") || org.contains("鹏博士") || org.contains("drpeng")) {
                return "鹏博士";
            }
            if (org.contains("founder") || org.contains("方正宽带") || org.contains("founder broadband")) {
                return "方正宽带";
            }
            if (org.contains("cstnet") || org.contains("科技网")) {
                return "科技网";
            }

            // 中国云服务商
            if (org.contains("aliyun") || org.contains("alibaba") || org.contains("阿里云")) {
                return "阿里云";
            }
            if (org.contains("tencent") || org.contains("qq") || org.contains("腾讯云")) {
                return "腾讯云";
            }
            if (org.contains("huawei") || org.contains("华为云")) {
                return "华为云";
            }
            if (org.contains("baidu") || org.contains("百度云")) {
                return "百度云";
            }
            if (org.contains("jd cloud") || org.contains("jdcloud") || org.contains("京东云")) {
                return "京东云";
            }
            if (org.contains("kingsoft") || org.contains("ksyun") || org.contains("金山云")) {
                return "金山云";
            }
            if (org.contains("ucloud") || org.contains("ucloud.cn")) {
                return "UCloud";
            }
            if (org.contains("qingcloud") || org.contains("青云")) {
                return "青云";
            }

            // 中国IDC服务商
            if (org.contains("21vianet") || org.contains("世纪互联")) {
                return "世纪互联";
            }
            if (org.contains("sinnet") || org.contains("光环新网")) {
                return "光环新网";
            }
            if (org.contains("wangsu") || org.contains("网宿科技")) {
                return "网宿科技";
            }
            if (org.contains("chinacache") || org.contains("蓝汛")) {
                return "蓝汛";
            }

            // 国际运营商
            if (org.contains("at&t") || org.contains("att.com")) {
                return "AT&T";
            }
            if (org.contains("verizon") || org.contains("verizon.com")) {
                return "Verizon";
            }
            if (org.contains("comcast") || org.contains("comcast.com")) {
                return "Comcast";
            }
            if (org.contains("t-mobile") || org.contains("tmobile.com")) {
                return "T-Mobile";
            }
            if (org.contains("sprint") || org.contains("sprint.com")) {
                return "Sprint";
            }
            if (org.contains("vodafone") || org.contains("vodafone.com")) {
                return "Vodafone";
            }
            if (org.contains("orange") || org.contains("orange.com")) {
                return "Orange";
            }
            if (org.contains("deutsche telekom") || org.contains("telekom.de")) {
                return "德国电信";
            }
            if (org.contains("bt") || org.contains("british telecom")) {
                return "英国电信";
            }

            // 亚太运营商
            if (org.contains("ntt") || org.contains("ntt.com")) {
                return "NTT";
            }
            if (org.contains("kddi") || org.contains("kddi.com")) {
                return "KDDI";
            }
            if (org.contains("softbank") || org.contains("softbank.jp")) {
                return "SoftBank";
            }
            if (org.contains("sk telecom") || org.contains("skt.com")) {
                return "SK Telecom";
            }
            if (org.contains("kt") || org.contains("kt.com")) {
                return "KT";
            }
            if (org.contains("lg uplus") || org.contains("lguplus.co.kr")) {
                return "LG Uplus";
            }
            if (org.contains("singtel") || org.contains("singtel.com")) {
                return "SingTel";
            }
            if (org.contains("starhub") || org.contains("starhub.com")) {
                return "StarHub";
            }
            if (org.contains("telstra") || org.contains("telstra.com")) {
                return "Telstra";
            }
            if (org.contains("optus") || org.contains("optus.com.au")) {
                return "Optus";
            }

            // 欧洲运营商
            if (org.contains("telefonica") || org.contains("telefonica.com")) {
                return "Telefonica";
            }
            if (org.contains("telecom italia") || org.contains("tim.it")) {
                return "Telecom Italia";
            }
            if (org.contains("swisscom") || org.contains("swisscom.ch")) {
                return "Swisscom";
            }
            if (org.contains("telenor") || org.contains("telenor.com")) {
                return "Telenor";
            }

            return "其他";
        }

        /**
         * 转换运营商信息为中文 - 增强版
         */
        private String convertISPToChinese(String isp) {
            if (isp == null || isp.isEmpty() || "未知".equals(isp)) {
                return "未知";
            }

            log.debug("转换运营商: {}", isp);

            // 如果是中文，直接返回
            if (isp.matches("[\\u4e00-\\u9fa5]+")) {
                // 中文运营商名称直接匹配
                if (isp.contains("电信")) {
                    return "电信";
                }
                if (isp.contains("联通")) {
                    return "联通";
                }
                if (isp.contains("移动")) {
                    return "移动";
                }
                if (isp.contains("铁通")) {
                    return "铁通";
                }
                if (isp.contains("长城宽带")) {
                    return "长城宽带";
                }
                if (isp.contains("教育网")) {
                    return "教育网";
                }
                if (isp.contains("广电")) {
                    return "广电";
                }
                if (isp.contains("鹏博士")) {
                    return "鹏博士";
                }
                if (isp.contains("方正宽带")) {
                    return "方正宽带";
                }
                if (isp.contains("科技网")) {
                    return "科技网";
                }
                return isp; // 直接返回中文名称
            }

            // 英文运营商识别
            isp = isp.toLowerCase();

            // 中国运营商
            if (isp.contains("china telecom") || isp.contains("telecom") || isp.contains("chinanet")) {
                return "电信";
            }
            if (isp.contains("china unicom") || isp.contains("unicom") || isp.contains("cnc")) {
                return "联通";
            }
            if (isp.contains("china mobile") || isp.contains("mobile") || isp.contains("cmcc") || isp.contains("cmnet")) {
                return "移动";
            }
            if (isp.contains("china tietong") || isp.contains("tietong") || isp.contains("ctt")) {
                return "铁通";
            }
            if (isp.contains("cernet") || isp.contains("edu")) {
                return "教育网";
            }
            if (isp.contains("great wall") || isp.contains("长城宽带") || isp.contains("gwbn")) {
                return "长城宽带";
            }
            if (isp.contains("cable") || isp.contains("广电")) {
                return "广电";
            }
            if (isp.contains("dr.peng") || isp.contains("鹏博士")) {
                return "鹏博士";
            }
            if (isp.contains("founder") || isp.contains("方正宽带")) {
                return "方正宽带";
            }
            if (isp.contains("cstnet") || isp.contains("科技网")) {
                return "科技网";
            }

            // 云服务商
            if (isp.contains("aliyun") || isp.contains("alibaba")) {
                return "阿里云";
            }
            if (isp.contains("tencent") || isp.contains("qq")) {
                return "腾讯云";
            }
            if (isp.contains("huawei")) {
                return "华为云";
            }
            if (isp.contains("baidu")) {
                return "百度云";
            }
            if (isp.contains("jd cloud") || isp.contains("jdcloud")) {
                return "京东云";
            }
            if (isp.contains("kingsoft") || isp.contains("ksyun")) {
                return "金山云";
            }
            if (isp.contains("ucloud")) {
                return "UCloud";
            }
            if (isp.contains("qingcloud")) {
                return "青云";
            }

            // 国际运营商映射为中文
            if (isp.contains("at&t") || isp.contains("att")) {
                return "AT&T";
            }
            if (isp.contains("verizon")) {
                return "Verizon";
            }
            if (isp.contains("comcast")) {
                return "Comcast";
            }
            if (isp.contains("t-mobile") || isp.contains("tmobile")) {
                return "T-Mobile";
            }
            if (isp.contains("sprint")) {
                return "Sprint";
            }
            if (isp.contains("vodafone")) {
                return "Vodafone";
            }
            if (isp.contains("orange")) {
                return "Orange";
            }
            if (isp.contains("deutsche telekom") || isp.contains("telekom")) {
                return "德国电信";
            }
            if (isp.contains("british telecom") || isp.contains("bt")) {
                return "英国电信";
            }

            // 亚太运营商
            if (isp.contains("ntt")) {
                return "NTT";
            }
            if (isp.contains("kddi")) {
                return "KDDI";
            }
            if (isp.contains("softbank")) {
                return "SoftBank";
            }
            if (isp.contains("sk telecom") || isp.contains("skt")) {
                return "SK Telecom";
            }
            if (isp.contains("kt")) {
                return "KT";
            }
            if (isp.contains("lg uplus")) {
                return "LG Uplus";
            }
            if (isp.contains("singtel")) {
                return "SingTel";
            }
            if (isp.contains("starhub")) {
                return "StarHub";
            }
            if (isp.contains("telstra")) {
                return "Telstra";
            }
            if (isp.contains("optus")) {
                return "Optus";
            }

            // 欧洲运营商
            if (isp.contains("telefonica")) {
                return "Telefonica";
            }
            if (isp.contains("telecom italia") || isp.contains("tim")) {
                return "Telecom Italia";
            }
            if (isp.contains("swisscom")) {
                return "Swisscom";
            }
            if (isp.contains("telenor")) {
                return "Telenor";
            }

            return "其他";
        }

        /**
         * 基于IP段猜测运营商 - 增强版
         */
        private String guessISPByIP(String ip) {
            if (ip == null) {
                return "未知";
            }

            try {
                String[] parts = ip.split("\\.");
                if (parts.length != 4) {
                    return "未知";
                }

                int firstOctet = Integer.parseInt(parts[0]);
                int secondOctet = Integer.parseInt(parts[1]);

                // 中国电信IP段
                if (firstOctet == 36 && secondOctet >= 128 && secondOctet <= 159) {
                    return "电信";
                }
                if (firstOctet == 39 && secondOctet >= 128 && secondOctet <= 191) {
                    return "电信";
                }
                if (firstOctet == 49 && secondOctet >= 64 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 58 && secondOctet >= 16 && secondOctet <= 63) {
                    return "电信";
                }
                if (firstOctet == 60 && secondOctet >= 0 && secondOctet <= 63) {
                    return "电信";
                }
                if (firstOctet == 110 && secondOctet >= 64 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 111 && secondOctet >= 0 && secondOctet <= 63) {
                    return "电信";
                }
                if (firstOctet == 112 && secondOctet >= 0 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 113 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 114 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 115 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 116 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 117 && secondOctet >= 0 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 118 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 119 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 171 && secondOctet >= 0 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 175 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 180 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 182 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 183 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 202 && secondOctet >= 96 && secondOctet <= 127) {
                    return "电信";
                }
                if (firstOctet == 210 && secondOctet >= 0 && secondOctet <= 79) {
                    return "电信";
                }
                if (firstOctet == 218 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 219 && secondOctet >= 128 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 220 && secondOctet >= 160 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 221 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }
                if (firstOctet == 222 && secondOctet >= 0 && secondOctet <= 255) {
                    return "电信";
                }

                // 中国移动IP段
                if (firstOctet == 36 && secondOctet >= 0 && secondOctet <= 127) {
                    return "移动";
                }
                if (firstOctet == 39 && secondOctet >= 0 && secondOctet <= 127) {
                    return "移动";
                }
                if (firstOctet == 111 && secondOctet >= 0 && secondOctet <= 127) {
                    return "移动";
                }
                if (firstOctet == 112 && secondOctet >= 0 && secondOctet <= 127) {
                    return "移动";
                }
                if (firstOctet == 117 && secondOctet >= 128 && secondOctet <= 191) {
                    return "移动";
                }
                if (firstOctet == 120 && secondOctet >= 192 && secondOctet <= 255) {
                    return "移动";
                }
                if (firstOctet == 183 && secondOctet >= 0 && secondOctet <= 255) {
                    return "移动";
                }
                if (firstOctet == 211 && secondOctet >= 96 && secondOctet <= 127) {
                    return "移动";
                }
                if (firstOctet == 223 && secondOctet >= 0 && secondOctet <= 255) {
                    return "移动";
                }

                // 中国联通IP段
                if (firstOctet == 36 && secondOctet >= 160 && secondOctet <= 191) {
                    return "联通";
                }
                if (firstOctet == 42 && secondOctet >= 48 && secondOctet <= 127) {
                    return "联通";
                }
                if (firstOctet == 58 && secondOctet >= 0 && secondOctet <= 15) {
                    return "联通";
                }
                if (firstOctet == 60 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 110 && secondOctet >= 0 && secondOctet <= 63) {
                    return "联通";
                }
                if (firstOctet == 111 && secondOctet >= 128 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 112 && secondOctet >= 128 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 113 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 114 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 115 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 116 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 117 && secondOctet >= 0 && secondOctet <= 127) {
                    return "联通";
                }
                if (firstOctet == 118 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 119 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 124 && secondOctet >= 64 && secondOctet <= 127) {
                    return "联通";
                }
                if (firstOctet == 171 && secondOctet >= 128 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 175 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 182 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 183 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 202 && secondOctet >= 96 && secondOctet <= 127) {
                    return "联通";
                }
                if (firstOctet == 210 && secondOctet >= 80 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 218 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 219 && secondOctet >= 0 && secondOctet <= 127) {
                    return "联通";
                }
                if (firstOctet == 220 && secondOctet >= 0 && secondOctet <= 159) {
                    return "联通";
                }
                if (firstOctet == 221 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }
                if (firstOctet == 222 && secondOctet >= 0 && secondOctet <= 255) {
                    return "联通";
                }

                // 特定IP段判断
                if (firstOctet == 36 && secondOctet == 157) {
                    return "移动"; // 36.157.x.x 属于移动
                }

            } catch (NumberFormatException e) {
                log.debug("IP格式解析失败: {}", ip);
            }

            return "未知";
        }

        /**
         * 检查字符串是否包含编码问题（乱码）
         */
        private boolean containsEncodingIssue(String str) {
            if (str == null || str.isEmpty()) {
                return false;
            }
            // 检查是否包含乱码字符（如问号方块等）
            return str.matches(".*[�?�].*") ||
                    (str.length() > 0 && str.chars().anyMatch(c -> c > 0x7F && c < 0xA0));
        }

        /**
         * 验证地理位置是否有效
         */
        private boolean isValidLocation(String province, String city) {
            return province != null && !province.trim().isEmpty() &&
                    city != null && !city.trim().isEmpty() &&
                    !"未知".equals(province) && !"未知".equals(city) &&
                    !province.trim().isEmpty() && !city.trim().isEmpty();
        }

        /**
         * 将省份英文名转换为中文
         */
        private String convertProvinceToChinese(String englishName) {
            if (englishName == null || englishName.isEmpty() || "未知".equals(englishName)) {
                return "未知";
            }

            // 如果是中文，直接返回
            if (englishName.matches("[\\u4e00-\\u9fa5]+")) {
                return englishName;
            }

            // 英文到中文的映射
            switch (englishName.toLowerCase()) {
                case "beijing":
                    return "北京市";
                case "tianjin":
                    return "天津市";
                case "hebei":
                    return "河北省";
                case "shanxi":
                    return "山西省";
                case "neimenggu":
                    return "内蒙古自治区";
                case "liaoning":
                    return "辽宁省";
                case "jilin":
                    return "吉林省";
                case "heilongjiang":
                    return "黑龙江省";
                case "shanghai":
                    return "上海市";
                case "jiangsu":
                    return "江苏省";
                case "zhejiang":
                    return "浙江省";
                case "anhui":
                    return "安徽省";
                case "fujian":
                    return "福建省";
                case "jiangxi":
                    return "江西省";
                case "shandong":
                    return "山东省";
                case "henan":
                    return "河南省";
                case "hubei":
                    return "湖北省";
                case "hunan":
                    return "湖南省";
                case "guangdong":
                    return "广东省";
                case "guangxi":
                    return "广西壮族自治区";
                case "hainan":
                    return "海南省";
                case "chongqing":
                    return "重庆市";
                case "sichuan":
                    return "四川省";
                case "guizhou":
                    return "贵州省";
                case "yunnan":
                    return "云南省";
                case "xizang":
                    return "西藏自治区";
                case "shaanxi":
                    return "陕西省";
                case "gansu":
                    return "甘肃省";
                case "qinghai":
                    return "青海省";
                case "ningxia":
                    return "宁夏回族自治区";
                case "xinjiang":
                    return "新疆维吾尔自治区";
                case "taiwan":
                    return "台湾省";
                case "hong kong":
                    return "香港特别行政区";
                case "macao":
                    return "澳门特别行政区";
                default:
                    log.debug("未知省份英文名: {}", englishName);
                    return "未知";
            }
        }

        /**
         * 将城市英文名转换为中文
         */
        private String convertCityToChinese(String englishName) {
            if (englishName == null || englishName.isEmpty() || "未知".equals(englishName)) {
                return "未知";
            }

            // 如果是中文，直接返回
            if (englishName.matches("[\\u4e00-\\u9fa5]+")) {
                return englishName;
            }

            // 主要城市英文到中文映射
            switch (englishName.toLowerCase()) {
                case "beijing":
                    return "北京";
                case "shanghai":
                    return "上海";
                case "tianjin":
                    return "天津";
                case "chongqing":
                    return "重庆";
                case "guangzhou":
                    return "广州";
                case "shenzhen":
                    return "深圳";
                case "hangzhou":
                    return "杭州";
                case "nanjing":
                    return "南京";
                case "wuhan":
                    return "武汉";
                case "chengdu":
                    return "成都";
                case "xian":
                    return "西安";
                case "suzhou":
                    return "苏州";
                case "dalian":
                    return "大连";
                case "qingdao":
                    return "青岛";
                case "ningbo":
                    return "宁波";
                case "xiamen":
                    return "厦门";
                default:
                    // 对于未知城市，返回原值但标记为未知
                    log.debug("未知城市英文名: {}", englishName);
                    return "未知";
            }
        }

        private String formatTimestamp(Long timestamp) {
            if (timestamp == null) {
                return "未知时间";
            }
            try {
                long timestampMs = timestamp.toString().length() == 10 ? timestamp * 1000L : timestamp;
                return DATE_FORMAT.format(new Date(timestampMs));
            } catch (Exception e) {
                log.debug("时间戳格式化失败: {}", timestamp);
                return "时间格式错误";
            }
        }
    }

    // ip-api.com 响应数据结构
    @Data
    public static class IpApiResponse {
        private String status;
        private String message;
        private String country;
        private String regionName;
        private String city;
        private String isp;
        private String query;
    }

    @Data
    public static class LogEvent {
        @JSONField(name = "log_id")
        private String logId;

        private Device device;
        private Gis gis;
        private Network network;
        private String opa;

        @JSONField(name = "log_type")
        private String logType;

        private Long ts;

        @JSONField(name = "product_id")
        private String productId;

        @JSONField(name = "order_id")
        private String orderId;

        @JSONField(name = "user_id")
        private String userId;

        private List<String> keywords;

        // Getter方法
        public String getLogId() {
            return logId;
        }

        public Device getDevice() {
            return device;
        }

        public Gis getGis() {
            return gis;
        }

        public Network getNetwork() {
            return network;
        }

        public String getLogType() {
            return logType;
        }

        public Long getTs() {
            return ts;
        }

        public String getProductId() {
            return productId;
        }

        public String getOrderId() {
            return orderId;
        }

        public String getUserId() {
            return userId;
        }

        @Data
        public static class Device {
            private String brand;

            public String getBrand() {
                return brand;
            }
        }

        @Data
        public static class Gis {
            private String ip;

            public String getIp() {
                return ip;
            }
        }

        @Data
        public static class Network {
            private String net;

            public String getNet() {
                return net;
            }
        }
    }

    @Data
    public static class ProcessedLogEvent {
        private String logId;
        private String ip;
        private String province;
        private String city;
        private String isp; // 新增运营商字段
        private String logType;
        private String userId;
        private String productId;
        private String orderId;
        private Long timestamp;
        private String formattedDate;
        private String brand;
        private String network;
        private String originalData;

        @Override
        public String toString() {
            return String.format("日志ID: %s | IP: %s | 省份: %s | 城市: %s | 运营商: %s | 类型: %s | 用户: %s | 品牌: %s | 网络: %s | 时间: %s",
                    logId, ip, province, city, isp, logType, userId, brand, network, formattedDate);
        }
    }
}