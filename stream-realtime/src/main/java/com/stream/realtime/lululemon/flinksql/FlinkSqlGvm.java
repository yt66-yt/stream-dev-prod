package com.stream.realtime.lululemon.flinksql;

import com.stream.core.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author A
 */
public class FlinkSqlGvm {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30 s");
        ZoneId sh = ZoneId.of("Asia/Shanghai");
        long today0Millis = LocalDate.now(sh)
                .atStartOfDay(sh)
                .toInstant()
                .toEpochMilli();

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

        // 1. 执行创建表的SQL
        tenv.executeSql(dll_kafka_oms_order_info);

        // 2. 修正查询SQL
        String compute0ToCurrentDaySaleAmountGMV =
                "select DATE_FORMAT(window_start, 'yyyy-MM-dd') AS order_date,      \n" +
                        "       window_start,                                       \n" +
                        "       window_end,                                         \n" +
                        "       SUM(CAST(total_amount AS DECIMAL(18,2))) AS gmv,    \n" +
                        "       'TODAY' AS type,                                    \n" +
                        "       window_end AS calc_time                             \n" +
                        "from TABLE(                                                \n" +
                        "    CUMULATE(                                              \n" +
                        "        TABLE t_kafka_oms_order_info,                      \n" +
                        "        DESCRIPTOR(ts_ms),                                 \n" +
                        "        INTERVAL '10' minutes,                             \n" +
                        "        INTERVAL '1' DAY                                   \n" +
                        "    )                                                      \n" +
                        ") where ts_ms >= floor(current_timestamp to day)           \n" +
                        "group by window_start, window_end;                    ";

        // 3. 执行查询
        tenv.executeSql(compute0ToCurrentDaySaleAmountGMV).print();






        env.execute("Lululemon Today Cumulative GMV Calculation");
    }
}