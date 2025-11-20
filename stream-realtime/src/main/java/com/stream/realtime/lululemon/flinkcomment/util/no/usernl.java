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

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * PostgreSQL用户年龄判断处理器
 */
public class usernl {

    // 日期格式常量
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

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

        // PostgreSQL连接配置
        String hostname = "pgsql_cdh01";
        String username = "sa";
        String password = "zhangyihao@123";
        String database = "spider_db";
        String schema = "public";
        String table = "spider_db_public_user_info_base";

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

        // 处理数据：解析JSON并判断年龄
        DataStream<UserInfoWithAge> processedStream = sourceStream.process(new ProcessFunction<String, UserInfoWithAge>() {
            @Override
            public void processElement(String json, Context context, Collector<UserInfoWithAge> collector) throws Exception {
                try {
                    UserInfoWithAge userInfo = parseFromJson(json);
                    if (userInfo != null) {
                        // 判断年龄
                        String age = calculateAge(userInfo.getBirthday());
                        userInfo.setAge(age);

                        System.out.println("处理用户: ID=" + userInfo.getUserId() +
                                ", 姓名=" + userInfo.getUname() +
                                ", 生日=" + userInfo.getBirthday() +
                                " -> 年龄=" + age);
                        collector.collect(userInfo);
                    }
                } catch (Exception e) {
                    System.err.println("处理数据错误: " + json);
                    e.printStackTrace();
                }
            }
        });

        // 输出到控制台
        processedStream.print();

        env.execute("PostgreSQL User Age Processor");
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

        // PostgreSQL连接配置
        String hostname = "your-postgres-host";
        String username = "your-username";
        String password = "your-password";
        String database = "spider_db";
        String schema = "public";
        String table = "spider_db_public_user_info_base";

        // 注册UDF函数
        tableEnv.createTemporarySystemFunction("CALCULATE_AGE", CalculateAgeUDF.class);

        // 创建PostgreSQL CDC表
        String sourceDDL = String.format(
                "CREATE TABLE user_info_source (\n" +
                        "  id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  uname STRING,\n" +
                        "  phone_num STRING,\n" +
                        "  birthday STRING,\n" +
                        "  gender STRING,\n" +
                        "  address STRING,\n" +
                        "  ts STRING,\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'postgres-cdc',\n" +
                        "  'hostname' = '%s',\n" +
                        "  'port' = '5432',\n" +
                        "  'username' = '%s',\n" +
                        "  'password' = '%s',\n" +
                        "  'database-name' = '%s',\n" +
                        "  'schema-name' = '%s',\n" +
                        "  'table-name' = '%s',\n" +
                        "  'decoding.plugin.name' = 'pgoutput'\n" +
                        ")",
                hostname, username, password, database, schema, table
        );

        tableEnv.executeSql(sourceDDL);

        // 创建结果表（输出到控制台）
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "  user_id STRING,\n" +
                "  uname STRING,\n" +
                "  birthday STRING,\n" +
                "  age STRING,\n" +
                "  gender STRING,\n" +
                "  address STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        tableEnv.executeSql(sinkDDL);

        // 执行查询并输出
        String querySQL = "INSERT INTO print_sink\n" +
                "SELECT \n" +
                "  user_id,\n" +
                "  uname,\n" +
                "  birthday,\n" +
                "  CALCULATE_AGE(birthday) as age,\n" +
                "  CASE \n" +
                "    WHEN gender = '0' THEN '女'\n" +
                "    WHEN gender = '1' THEN '男'\n" +
                "    ELSE '未知'\n" +
                "  END as gender,\n" +
                "  address\n" +
                "FROM user_info_source";

        tableEnv.executeSql(querySQL);

        env.execute("PostgreSQL User Age Table API Processor");
    }

    /**
     * 从Debezium JSON格式解析为UserInfoWithAge对象
     */
    private static UserInfoWithAge parseFromJson(String json) {
        try {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject after = jsonObject.getJSONObject("after");

            if (after == null) {
                return null; // 删除操作等没有after数据
            }

            UserInfoWithAge userInfo = new UserInfoWithAge();

            // 解析字段
            userInfo.setId(after.getString("id"));
            userInfo.setUserId(after.getString("user_id"));
            userInfo.setUname(after.getString("uname"));
            userInfo.setPhoneNum(after.getString("phone_num"));
            userInfo.setBirthday(after.getString("birthday"));
            userInfo.setGender(after.getString("gender"));
            userInfo.setAddress(after.getString("address"));
            userInfo.setTs(after.getString("ts"));

            return userInfo;

        } catch (Exception e) {
            System.err.println("JSON解析错误: " + json);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 计算年龄
     * 如果birthday字段有问题，返回"未知"
     */
    private static String calculateAge(String birthday) {
        if (birthday == null || birthday.isEmpty() || birthday.equals("None")) {
            return "未知";
        }

        try {
            // 检查日期格式
            if (!DATE_PATTERN.matcher(birthday).matches()) {
                return "未知";
            }

            // 解析生日日期
            LocalDate birthDate = LocalDate.parse(birthday, DATE_FORMATTER);
            LocalDate currentDate = LocalDate.now();

            // 检查日期是否合理（不能是未来日期）
            if (birthDate.isAfter(currentDate)) {
                return "未知";
            }

            // 计算年龄
            Period period = Period.between(birthDate, currentDate);
            int age = period.getYears();

            // 检查年龄是否合理（0-150岁）
            if (age < 0 || age > 150) {
                return "未知";
            }

            return String.valueOf(age);

        } catch (DateTimeParseException e) {
            System.err.println("日期解析错误: " + birthday + " - " + e.getMessage());
            return "未知";
        } catch (Exception e) {
            System.err.println("年龄计算错误: " + birthday + " - " + e.getMessage());
            return "未知";
        }
    }

    /**
     * 用户信息实体类（包含年龄字段）
     */
    @Data
    public static class UserInfoWithAge {
        private String id;
        private String userId;
        private String uname;
        private String phoneNum;
        private String birthday;
        private String gender;
        private String address;
        private String ts;
        private String age; // 新增字段：年龄
    }

    /**
     * Table API UDF函数 - 年龄计算
     */
    public static class CalculateAgeUDF extends ScalarFunction {
        public String eval(String birthday) {
            return calculateAge(birthday);
        }
    }
}