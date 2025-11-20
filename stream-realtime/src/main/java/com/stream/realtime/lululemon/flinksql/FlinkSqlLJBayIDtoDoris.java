package com.stream.realtime.lululemon.flinksql;

import com.stream.core.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import lombok.var;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 窗口内的累计购买的ID写入doris
 */
public class FlinkSqlLJBayIDtoDoris {

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
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
        String source_kafka_order_info_ddl = "create table if not exists t_kafka_oms_order_info (\n" +
                "    id string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    user_name string,\n" +
                "    phone_number string,\n" +
                "    product_link string,\n" +
                "    product_id string,\n" +
                "    color string,\n" +
                "    size string,\n" +
                "    item_id string,\n" +
                "    material string,\n" +
                "    sale_num string,\n" +
                "    sale_amount string,\n" +
                "    total_amount string,\n" +
                "    product_name string,\n" +
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts bigint,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    insert_time string,\n" +
                "    table_name string,\n" +
                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '5' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info1',\n" +
                "    'properties.bootstrap.servers'= '172.24.158.53:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);
        System.out.println("✅ Kafka源表创建成功");

        // 2. 创建Doris目标表 - 优化参数
        String dorisSinkDDL = "CREATE TABLE IF NOT EXISTS doris_order_stats (\n" +
                "    order_date STRING,\n" +
                "    window_start STRING,\n" +
                "    window_end STRING,\n" +
                "    gmv DECIMAL(18,2),\n" +
                "    top5_product_ids STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '192.168.200.30:18030',\n" +
                "    'table.identifier' = 'realtime.order_stats',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'sink.buffer-flush.max-rows' = '1',\n" +  // 设置为1立即刷新
                "    'sink.buffer-flush.interval' = '1000',\n" +  // 缩短间隔
                "    'sink.max-retries' = '5',\n" +
                "    'sink.properties.format' = 'json',\n" +
                "    'sink.properties.read_json_by_line' = 'true',\n" +
                "    'sink.properties.strip_outer_array' = 'true',\n" +  // 重要参数
                "    'sink.enable-2pc' = 'false'\n" +
                ")";

        try {
            tenv.executeSql(dorisSinkDDL);
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
            return;
        }

//        // 4. 测试单条数据写入Doris
//        System.out.println("=== 测试单条数据写入Doris ===");
//        String testInsert = "INSERT INTO doris_top5_results VALUES ('2025-10-30', '2025-10-30 09:00:00', '2025-10-30 09:10:00', 1000.00, 'test_product_001,test_product_002')";
//        try {
//            tenv.executeSql(testInsert);
//            System.out.println("✅ 测试数据写入成功");
//            Thread.sleep(3000); // 等待测试数据写入
//        } catch (Exception e) {
//            System.err.println("❌ 测试数据写入失败: " + e.getMessage());
//            e.printStackTrace();
//        }

        // 5. 创建临时视图
        String createViewSql = "CREATE TEMPORARY VIEW result_view AS\n" +
                "SELECT \n" +
                "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd') as order_date, \n" +
                "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd HH:mm:ss') as window_start, \n" +
                "    DATE_FORMAT(wg.window_end, 'yyyy-MM-dd HH:mm:ss') as window_end, \n" +
                "    wg.total_gmv as gmv,\n" +
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
                "    WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-28'\n" +
                "    GROUP BY window_start, window_end\n" +
                ") wg\n" +
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
                "        WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-28'\n" +
                "        GROUP BY window_start, window_end, product_id\n" +
                "    )\n" +
                "    WHERE rn <= 5\n" +
                "    GROUP BY window_start, window_end\n" +
                ") tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end";

        try {
            tenv.executeSql(createViewSql);
            System.out.println("✅ 临时视图创建成功");
        } catch (Exception e) {
            System.err.println("❌ 临时视图创建失败: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 6. 预览数据
        System.out.println("=== 预览查询结果 ===");
        try {
            tenv.executeSql("SELECT * FROM result_view LIMIT 5").print();
            System.out.println("✅ 数据预览成功");
        } catch (Exception e) {
            System.err.println("❌ 数据预览失败: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 7. 插入数据到Doris
        System.out.println("=== 开始写入数据到Doris ===");
        try {
            var result = tenv.executeSql("INSERT INTO order_stats SELECT * FROM result_view");

            // 获取作业客户端
            var jobClient = result.getJobClient();
            if (jobClient.isPresent()) {
                System.out.println("✅ 数据写入任务提交成功，作业ID: " + jobClient.get().getJobID());

                // 等待数据写入
                System.out.println("⏳ 等待数据写入Doris（15秒）...");
                Thread.sleep(15000);

                // 检查作业状态
                try {
                    var jobStatus = jobClient.get().getJobStatus().get();
                    System.out.println("📊 作业状态: " + jobStatus);
                } catch (Exception e) {
                    System.out.println("⚠️ 无法获取作业状态: " + e.getMessage());
                }
            } else {
                System.out.println("✅ 数据写入任务提交成功");
            }

        } catch (Exception e) {
            System.err.println("❌ 数据写入失败: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 8. 执行作业
        System.out.println("=== 开始执行Flink作业 ===");
        try {
            env.execute("Flink SQL Top5 to Doris");
            System.out.println("✅ Flink作业执行完成");
        } catch (Exception e) {
            System.err.println("❌ Flink作业执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}