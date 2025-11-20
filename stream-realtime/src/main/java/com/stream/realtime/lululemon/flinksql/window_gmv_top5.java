package com.stream.realtime.lululemon.flinksql;

import com.stream.core.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 窗口内销量Top5的商品ID写入doris
 */
public class window_gmv_top5 {

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

        // 1. 创建Kafka源表
        String source_kafka_order_info_ddl = "CREATE TABLE IF NOT EXISTS t_kafka_oms_order_info (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    user_name STRING,\n" +
                "    phone_number STRING,\n" +
                "    product_link STRING,\n" +
                "    product_id STRING,\n" +
                "    color STRING,\n" +
                "    size STRING,\n" +
                "    item_id STRING,\n" +
                "    material STRING,\n" +
                "    sale_num STRING,\n" +
                "    sale_amount STRING,\n" +
                "    total_amount STRING,\n" +
                "    product_name STRING,\n" +
                "    is_online_sales STRING,\n" +
                "    shipping_address STRING,\n" +
                "    recommendations_product_ids STRING,\n" +
                "    ds STRING,\n" +
                "    ts BIGINT,\n" +
                "    ts_ms AS CASE WHEN ts < 100000000000 THEN TO_TIMESTAMP_LTZ(ts * 1000, 3) ELSE TO_TIMESTAMP_LTZ(ts, 3) END,\n" +
                "    insert_time STRING,\n" +
                "    table_name STRING,\n" +
                "    op STRING,\n" +
                "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info1',\n" +
                "    'properties.bootstrap.servers' = '172.24.158.53:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);
        System.out.println("✅ Kafka源表创建成功");

        // 2. 创建Doris目标表 - 使用正确的参数
        String dorisSinkDdl = "CREATE TABLE IF NOT EXISTS doris_window_gmv_top5 (\n" +
                "    order_date STRING,\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3),\n" +
                "    gmv DECIMAL(18,2),\n" +
                "    top5_ids STRING,\n" +
                "    top5_product_ids STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '192.168.200.30:18030',\n" +
                "    'table.identifier' = 'realtime.window_gmv_top5',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'sink.buffer-flush.max-rows' = '1000',\n" +  // 修正的参数名
                "    'sink.buffer-flush.interval' = '1000ms',\n" +  // 修正的参数名
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
            // 使用简单的查询测试连接
            tenv.executeSql("SELECT 'test' as connection_test").print();
            System.out.println("✅ Doris连接测试成功");
        } catch (Exception e) {
            System.err.println("❌ Doris连接测试失败: " + e.getMessage());
        }

        // 4. 先测试简单的数据插入到Doris
//        System.out.println("=== 插入测试数据到Doris ===");
//        String testDataSql = "INSERT INTO doris_window_gmv_top5\n" +
//                "SELECT \n" +
//                "    '2025-10-26' as order_date,\n" +
//                "    TIMESTAMP '2025-10-26 10:00:00' as window_start,\n" +
//                "    TIMESTAMP '2025-10-26 10:10:00' as window_end,\n" +
//                "    CAST(1000.50 AS DECIMAL(18,2)) as gmv,\n" +
//                "    'test_id_1,test_id_2' as top5_ids,\n" +
//                "    'prod_1,prod_2' as top5_product_ids";
//
//        try {
//            tenv.executeSql(testDataSql);
//            System.out.println("✅ 测试数据插入任务提交成功");
//        } catch (Exception e) {
//            System.err.println("❌ 测试数据插入失败: " + e.getMessage());
//            e.printStackTrace();
//        }

        // 5. 等待一会儿
        Thread.sleep(3000);

        // 6. 主查询逻辑
        System.out.println("=== 执行主查询逻辑 ===");
        String mainQuery =
                "SELECT \n" +
                        "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd') as order_date, \n" +
                        "    wg.window_start, \n" +
                        "    wg.window_end, \n" +
                        "    wg.total_gmv as GMV,\n" +
                        "    COALESCE(ti.top5_ids, '') as top5_ids,\n" +
                        "    COALESCE(tp.top5_product_ids, '') as top5_product_ids\n" +
                        "FROM (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        SUM(CASE \n" +
                        "            WHEN TRY_CAST(total_amount AS DECIMAL(18,2)) IS NOT NULL THEN CAST(total_amount AS DECIMAL(18,2))\n" +
                        "            ELSE CAST(0 AS DECIMAL(18,2))\n" +
                        "        END) as total_gmv\n" +
                        "    FROM TABLE(\n" +
                        "        CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "    )\n" +
                        "    WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-26'\n" +
                        "    GROUP BY window_start, window_end\n" +
                        ") wg\n" +
                        "LEFT JOIN (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(id, ',') as top5_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CASE \n" +
                        "                    WHEN TRY_CAST(total_amount AS DECIMAL(18,2)) IS NOT NULL THEN CAST(total_amount AS DECIMAL(18,2))\n" +
                        "                    ELSE CAST(0 AS DECIMAL(18,2))\n" +
                        "                END) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-26'\n" +
                        "        GROUP BY window_start, window_end, id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        ") ti ON wg.window_start = ti.window_start AND wg.window_end = ti.window_end\n" +
                        "LEFT JOIN (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(product_id, ',') as top5_product_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            product_id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CASE \n" +
                        "                    WHEN TRY_CAST(total_amount AS DECIMAL(18,2)) IS NOT NULL THEN CAST(total_amount AS DECIMAL(18,2))\n" +
                        "                    ELSE CAST(0 AS DECIMAL(18,2))\n" +
                        "                END) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-26'\n" +
                        "        GROUP BY window_start, window_end, product_id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        ") tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end";

        // 7. 插入主数据到Doris
        System.out.println("=== 插入主数据到Doris ===");
        String insertMainSql = "INSERT INTO doris_window_gmv_top5 " + mainQuery;

        try {
            tenv.executeSql(insertMainSql);
            System.out.println("✅ 主数据插入任务提交成功");
        } catch (Exception e) {
            System.err.println("❌ 主数据插入失败: " + e.getMessage());
            e.printStackTrace();
        }

        // 8. 执行作业
        System.out.println("=== 开始执行Flink作业 ===");
        try {
            env.execute("Flink SQL to Doris - Window GMV Top5");
            System.out.println("✅ Flink作业执行完成");
        } catch (Exception e) {
            System.err.println("❌ Flink作业执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}