package com.stream.realtime.lululemon.func;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;



public class MapMergeJsonData extends RichMapFunction<JSONObject, JSONObject> {

    @Override
    public JSONObject map(JSONObject data) throws Exception {
        JSONObject resultJson = new JSONObject();
        if (data.containsKey("after") && data.getJSONObject("after") != null) {
            JSONObject after = data.getJSONObject("after");
            resultJson.putAll(after);

            // 添加表名信息
            if (data.containsKey("source")) {
                JSONObject source = data.getJSONObject("source");
                String db = source.getString("db");
                String schema = source.getString("schema");
                String table = source.getString("table");
                resultJson.put("table_name", db + "." + schema + "." + table);
                resultJson.put("source", source);
                // 保留完整源信息
            }

            if (data.containsKey("op")) {
                resultJson.put("op", data.getString("op"));
            }
            if (data.containsKey("ts_ms")) {
                resultJson.put("ts_ms", data.getLong("ts_ms"));
            }
        }
        return resultJson;
    }
}
