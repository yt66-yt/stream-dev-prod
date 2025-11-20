package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;


import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;

import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DbusSyncSqlserverOmsSysData2kafka {

    private static final String sql_to_kf = "realtime_v3_order_info1";
    private static final String sql_to_kafka_toip = "172.24.158.53:9092";

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        // 添加安全的主题创建逻辑
        try {
            System.out.println("检查Kafka主题状态...");
            // 如果KafkaUtils没有topicExists方法，使用try-catch包装创建逻辑
            KafkaUtils.createKafkaTopic(sql_to_kafka_toip, sql_to_kf, 3, (short) 1, true);
            System.out.println("Kafka主题处理完成: " + sql_to_kf);
        } catch (Exception e) {
            System.err.println("Kafka主题处理警告: " + e.getMessage());
            System.err.println("继续执行程序...");
            // 不退出程序，继续执行
        }


        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("192.168.200.30")
                .port(1433)
                .username("sa")
                .password("zhangyihao@123")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDs")
                .name("convertStr2JsonDs");

        SingleOutputStreamOperator<JSONObject> processedData = convertStr2JsonDs.map(new MapMergeJsonData());
        processedData.print("map_flit_to_kafka");

        // 构建Kafka Sink并将数据写入Kafka
        KafkaSink<String> kafkaSink = KafkaUtils.buildKafkaSink(sql_to_kafka_toip,sql_to_kf);

        SingleOutputStreamOperator<String> kafkaDataStream = processedData
                .map(json ->json.toJSONString())
                .uid("convertToJsonString")
                .name("convertToJsonString");

        kafkaDataStream.sinkTo(kafkaSink)
                .uid("kafkaSink")
                .name("kafkaSink");

        env.execute("sqlserver-cdc-test");
    }
}


