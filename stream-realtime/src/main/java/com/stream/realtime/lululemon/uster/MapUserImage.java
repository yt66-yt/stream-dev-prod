package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 处理 realtime_v3_logs 用户画像日志
 */
public class MapUserImage implements MapFunction<String, JSONObject> {

    @Override
    public JSONObject map(String value) {
        try {
            if (value == null || value.trim().isEmpty()) {
                System.out.println("⚠️ MapUserImage: 输入为空");
                return null;
            }

            JSONObject obj = JSON.parseObject(value);
            if (obj == null) {
                System.out.println("⚠️ MapUserImage: JSON解析失败");
                return null;
            }

            // 提取核心字段并统一转为小写
            String userId = obj.getString("user_id");
            if (userId != null) {
                userId = userId.toLowerCase(); // 统一转为小写
            } else {
                return null;
            }

            JSONObject result = new JSONObject();
            result.put("user_id", userId);
            result.put("product_id", obj.getString("product_id"));
            result.put("order_id", obj.getString("order_id"));
            result.put("ts", obj.getLong("ts"));

            System.out.println("✅ MapUserImage 处理成功: user_id=" + userId);

            return result;
        } catch (Exception e) {
            System.err.println("❌ MapUserImage 解析错误: " + e.getMessage() + ", 数据: " + value);
            return null;
        }
    }
}