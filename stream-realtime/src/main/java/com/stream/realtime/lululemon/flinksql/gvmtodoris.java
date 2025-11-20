package com.stream.realtime.lululemon.flinksql;

import com.stream.core.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author A
 * 累加金额写入doris
 */
public class gvmtodoris {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage());

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30 s");

        ZoneId sh = ZoneId.of("Asia/Shanghai");
        long today0Millis = LocalDate.now(sh)
                .atStartOfDay(sh)
                .toInstant()
                .toEpochMilli();

        // 1. 创建Kafka源表
        String dll_kafka_oms_order_info = String.format("create table if not exists t_kafka_oms_order_info (                    \n" +
                "    id string,                                                                                                 \n" +
                "    order_id string,                                                                                           \n" +
                "    user_id string,                                                                                            \n" +
                "    user_name string,                                                                                          \n" +
                "    phone_number string,                                                                                       \n" +
                "    product_link string,                                                                                       \n" +
                "    product_id string,                                                                                         \n" +
                "    color string,                                                                                              \n" +
                "    size string,                                                                                               \n" +
                "    item_id string,                                                                                            \n" +
                "    material string,                                                                                           \n" +
                "    sale_num string,                                                                                           \n" +
                "    sale_amount string,                                                                                        \n" +
                "    total_amount string,                                                                                       \n" +
                "    product_name string,                                                                                       \n" +
                "    is_online_sales string,                                                                                    \n" +
                "    shipping_address string,                                                                                   \n" +
                "    recommendations_product_ids string,                                                                        \n" +
                "    ds string,                                                                                                 \n" +
                "    ts bigint,                                                                                                 \n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end, \n" +
                "    insert_time string,                                                                                        \n" +
                "    table_name string,                                                                                         \n" +
                "    op string,                                                                                                 \n" +
                "    watermark for ts_ms as ts_ms - interval '5' second                                                         \n" +
                ")                                                                                                              \n" +
                "with (                                                                                                         \n" +
                "    'connector' = 'kafka',                                                                                     \n" +
                "    'topic' = 'realtime_v3_order_info1',                                                                       \n" +
                "    'properties.bootstrap.servers'= '172.24.158.53:9092',                                                      \n" +
                "    'properties.group.id' = 'order-analysis1',                                                                 \n" +
                "    'scan.startup.mode' = 'timestamp',                                                                         \n" +
                "    'scan.startup.timestamp-millis' = '%d',                                                                    \n" +
                "    'format' = 'json',                                                                                         \n" +
                "    'json.fail-on-missing-field' = 'false',                                                                    \n" +
                "    'json.ignore-parse-errors' = 'true'                                                                        \n" +
                ")", today0Millis);

        tenv.executeSql(dll_kafka_oms_order_info);
        System.out.println("✅ Kafka源表创建成功");

        // 2. 创建Doris目标表 - 用于存储GMV数据
        String dorisSinkDdl = "CREATE TABLE IF NOT EXISTS doris_gmv_daily (\n" +
                "    order_date DATE,\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
                "    gmv DECIMAL(18,2),\n" +
                "    type STRING,\n" +
                "    calc_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '192.168.200.30:18030',\n" +
                "    'table.identifier' = 'realtime.gmv_daily',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'sink.buffer-flush.max-rows' = '1000',\n" +
                "    'sink.buffer-flush.interval' = '1000ms',\n" +
                "    'sink.max-retries' = '3',\n" +
                "    'sink.properties.format' = 'json',\n" +
                "    'sink.properties.read_json_by_line' = 'true',\n" +
                "    'sink.enable-2pc' = 'false'\n" +
                ")";

        try {
            tenv.executeSql(dorisSinkDdl);
            System.out.println("✅ Doris目标表创建成功");
        } catch (Exception e) {
            System.err.println("❌ Doris目标表创建失败: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 3. 测试Doris连接
        System.out.println("=== 测试Doris连接 ===");
        try {
            tenv.executeSql("SELECT 'test' as connection_test").print();
            System.out.println("✅ Doris连接测试成功");
        } catch (Exception e) {
            System.err.println("❌ Doris连接测试失败: " + e.getMessage());
        }

        // 4. 计算GMV的SQL查询
        String computeGmvSql =
                "SELECT \n" +
                        "    CAST(DATE_FORMAT(window_start, 'yyyy-MM-dd') AS DATE) AS order_date, \n" +
                        "    window_start, \n" +
                        "    window_end, \n" +
                        "    SUM(CASE \n" +
                        "        WHEN TRY_CAST(total_amount AS DECIMAL(18,2)) IS NOT NULL THEN CAST(total_amount AS DECIMAL(18,2))\n" +
                        "        ELSE CAST(0 AS DECIMAL(18,2))\n" +
                        "    END) AS gmv, \n" +
                        "    'TODAY' AS type, \n" +
                        "    window_end AS calc_time \n" +
                        "FROM TABLE( \n" +
                        "    CUMULATE( \n" +
                        "        TABLE t_kafka_oms_order_info, \n" +
                        "        DESCRIPTOR(ts_ms), \n" +
                        "        INTERVAL '10' MINUTES, \n" +
                        "        INTERVAL '1' DAY \n" +
                        "    ) \n" +
                        ") \n" +
                        "WHERE ts_ms >= FLOOR(CURRENT_TIMESTAMP TO DAY) \n" +
                        "GROUP BY window_start, window_end";

        // 5. 插入数据到Doris
        System.out.println("=== 插入GMV数据到Doris ===");
        String insertGmvSql = "INSERT INTO doris_gmv_daily " + computeGmvSql;

        try {
            tenv.executeSql(insertGmvSql);
            System.out.println("✅ GMV数据插入任务提交成功");
        } catch (Exception e) {
            System.err.println("❌ GMV数据插入失败: " + e.getMessage());
            e.printStackTrace();
        }

        // 6. 执行作业
        System.out.println("=== 开始执行Flink作业 ===");
        try {
            env.execute("Flink SQL GMV to Doris - Daily Cumulative GMV");
            System.out.println("✅ Flink作业执行完成");
        } catch (Exception e) {
            System.err.println("❌ Flink作业执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}