package com.stream.realtime.lululemon.flinkcomment.util.no;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 单类解决方案：实时读取SQL Server表，拆分product_name字段生成category
 */
public class plcf {

    // 将Pattern定义为常量
    private static final Pattern CATEGORY_PATTERN = Pattern.compile("丨([^丨]*?)(?=[\\u4e00-\\u9fa5])");

    public static void main(String[] args) throws Exception {
        // 使用DataStream API方式
        runDataStreamJob();

        // 或者使用Table API方式（注释掉上面，取消下面注释）
        // runTableApiJob();
    }

    /**
     * 方式1: 使用DataStream API处理
     */
    public static void runDataStreamJob() throws Exception {
        // Flink环境配置
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(1);

        // SQL Server连接配置
        String hostname = "your-sqlserver-host";
        String username = "your-username";
        String password = "your-password";
        String database = "realtime_v3";
        String schema = "dbo";
        String table = "oms_order_dtl";

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

        // 处理数据：解析JSON并提取category
        DataStream<OrderDetail> processedStream = sourceStream.process(new ProcessFunction<String, OrderDetail>() {
            @Override
            public void processElement(String json, Context context, Collector<OrderDetail> collector) throws Exception {
                try {
                    OrderDetail order = parseFromJson(json);
                    if (order != null && order.getProductName() != null) {
                        // 提取category
                        String category = extractCategory(order.getProductName());
                        order.setCategory(category);

                        System.out.println("处理记录: ID=" + order.getId() + ", 产品=" + order.getProductName() + " -> 分类=" + category);
                        collector.collect(order);
                    }
                } catch (Exception e) {
                    System.err.println("处理数据错误: " + json);
                    e.printStackTrace();
                }
            }
        });

        // 输出到控制台
        processedStream.print();

        env.execute("SQL Server Product Name 实时处理任务");
    }

    /**
     * 方式2: 使用Table API处理
     */
    public static void runTableApiJob() throws Exception {
        // Flink环境配置
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // SQL Server连接配置
        String hostname = "your-sqlserver-host";
        String username = "your-username";
        String password = "your-password";
        String database = "realtime_v3";
        String schema = "dbo";
        String table = "oms_order_dtl";

        // 注册UDF函数
        tableEnv.createTemporarySystemFunction("EXTRACT_CATEGORY", ExtractCategoryUDF.class);

        // 创建SQL Server CDC表
        String sourceDDL = String.format(
                "CREATE TABLE oms_order_dtl_source (\n" +
                        "  id INT,\n" +
                        "  order_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  user_name STRING,\n" +
                        "  phone_number STRING,\n" +
                        "  product_link STRING,\n" +
                        "  product_id STRING,\n" +
                        "  color STRING,\n" +
                        "  size STRING,\n" +
                        "  item_id STRING,\n" +
                        "  material STRING,\n" +
                        "  sale_num INT,\n" +
                        "  sale_amount DECIMAL(10,2),\n" +
                        "  total_amount DECIMAL(10,2),\n" +
                        "  product_name STRING,\n" +
                        "  is_online_sales STRING,\n" +
                        "  shipping_address STRING,\n" +
                        "  recommendations_product_ids STRING,\n" +
                        "  ds STRING,\n" +
                        "  ts STRING,\n" +
                        "  insert_time BIGINT,\n" +  // 改为BIGINT类型处理时间戳
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'sqlserver-cdc',\n" +
                        "  'hostname' = '%s',\n" +
                        "  'port' = '1433',\n" +
                        "  'username' = '%s',\n" +
                        "  'password' = '%s',\n" +
                        "  'database-name' = '%s',\n" +
                        "  'schema-name' = '%s',\n" +
                        "  'table-name' = '%s'\n" +
                        ")",
                hostname, username, password, database, schema, table
        );

        tableEnv.executeSql(sourceDDL);

        // 创建结果表（输出到控制台）
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "  id INT,\n" +
                "  order_id STRING,\n" +
                "  product_name STRING,\n" +
                "  category STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        tableEnv.executeSql(sinkDDL);

        // 执行查询并输出
        String querySQL = "INSERT INTO print_sink\n" +
                "SELECT \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  product_name,\n" +
                "  EXTRACT_CATEGORY(product_name) as category\n" +
                "FROM oms_order_dtl_source";

        tableEnv.executeSql(querySQL);

        env.execute("SQL Server Product Name Table API 任务");
    }

    /**
     * 从Debezium JSON格式解析为OrderDetail对象
     */
    private static OrderDetail parseFromJson(String json) {
        try {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject after = jsonObject.getJSONObject("after");

            if (after == null) {
                return null; // 删除操作等没有after数据
            }

            OrderDetail order = new OrderDetail();

            // 解析字段
            order.setId(after.getInteger("id"));
            order.setOrderId(after.getString("order_id"));
            order.setUserId(after.getString("user_id"));
            order.setUserName(after.getString("user_name"));
            order.setPhoneNumber(after.getString("phone_number"));
            order.setProductLink(after.getString("product_link"));
            order.setProductId(after.getString("product_id"));
            order.setColor(after.getString("color"));
            order.setSize(after.getString("size"));
            order.setItemId(after.getString("item_id"));
            order.setMaterial(after.getString("material"));

            // 处理数字字段
            String saleNumStr = after.getString("sale_num");
            if (saleNumStr != null && !saleNumStr.equals("None")) {
                try {
                    order.setSaleNum(Integer.parseInt(saleNumStr));
                } catch (NumberFormatException e) {
                    order.setSaleNum(0);
                }
            }

            String saleAmountStr = after.getString("sale_amount");
            if (saleAmountStr != null && !saleAmountStr.equals("None")) {
                try {
                    order.setSaleAmount(new BigDecimal(saleAmountStr));
                } catch (NumberFormatException e) {
                    order.setSaleAmount(BigDecimal.ZERO);
                }
            }

            String totalAmountStr = after.getString("total_amount");
            if (totalAmountStr != null && !totalAmountStr.equals("None")) {
                try {
                    order.setTotalAmount(new BigDecimal(totalAmountStr));
                } catch (NumberFormatException e) {
                    order.setTotalAmount(BigDecimal.ZERO);
                }
            }

            order.setProductName(after.getString("product_name"));
            order.setIsOnlineSales(after.getString("is_online_sales"));
            order.setShippingAddress(after.getString("shipping_address"));
            order.setRecommendationsProductIds(after.getString("recommendations_product_ids"));
            order.setDs(after.getString("ds"));
            order.setTs(after.getString("ts"));

            // 修复时间戳解析问题
            Object insertTimeObj = after.get("insert_time");
            if (insertTimeObj != null) {
                try {
                    if (insertTimeObj instanceof Long) {
                        // 如果是Long类型的时间戳
                        long timestamp = (Long) insertTimeObj;
                        // 判断是秒级还是毫秒级时间戳
                        if (timestamp > 1000000000000L) {
                            // 毫秒级时间戳
                            order.setInsertTime(new Timestamp(timestamp));
                        } else {
                            // 秒级时间戳
                            order.setInsertTime(new Timestamp(timestamp * 1000));
                        }
                    } else if (insertTimeObj instanceof String) {
                        String insertTimeStr = (String) insertTimeObj;
                        if (!insertTimeStr.equals("None")) {
                            try {
                                long timestamp = Long.parseLong(insertTimeStr);
                                if (timestamp > 1000000000000L) {
                                    order.setInsertTime(new Timestamp(timestamp));
                                } else {
                                    order.setInsertTime(new Timestamp(timestamp * 1000));
                                }
                            } catch (NumberFormatException e) {
                                // 如果不是数字，尝试按日期字符串解析
                                try {
                                    // 处理可能的T分隔符
                                    String normalizedTime = insertTimeStr.replace('T', ' ').split("\\.")[0];
                                    order.setInsertTime(Timestamp.valueOf(normalizedTime));
                                } catch (IllegalArgumentException ex) {
                                    System.err.println("无法解析时间戳: " + insertTimeStr);
                                    order.setInsertTime(null);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("时间戳解析错误: " + insertTimeObj);
                    order.setInsertTime(null);
                }
            }

            return order;

        } catch (Exception e) {
            System.err.println("JSON解析错误: " + json);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 从product_name中提取category
     * 规则：从"丨"开始到第一个汉字结束
     */
    private static String extractCategory(String productName) {
        if (productName == null || productName.isEmpty()) {
            return null;
        }

        // 使用常量CATEGORY_PATTERN
        Matcher matcher = CATEGORY_PATTERN.matcher(productName);

        if (matcher.find()) {
            return matcher.group(1).trim();
        }

        return null;
    }

    /**
     * 订单详情实体类
     */
    @Data
    public static class OrderDetail {
        private Integer id;
        private String orderId;
        private String userId;
        private String userName;
        private String phoneNumber;
        private String productLink;
        private String productId;
        private String color;
        private String size;
        private String itemId;
        private String material;
        private Integer saleNum;
        private BigDecimal saleAmount;
        private BigDecimal totalAmount;
        private String productName;
        private String isOnlineSales;
        private String shippingAddress;
        private String recommendationsProductIds;
        private String ds;
        private String ts;
        private Timestamp insertTime;
        private String category; // 新增字段
    }

    /**
     * Table API UDF函数
     */
    public static class ExtractCategoryUDF extends ScalarFunction {
        public String eval(String productName) {
            return extractCategory(productName);
        }
    }
}