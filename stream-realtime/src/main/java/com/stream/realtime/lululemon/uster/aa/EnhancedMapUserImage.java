// EnhancedMapUserImage.java
package com.stream.realtime.lululemon.uster.aa;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

public class EnhancedMapUserImage implements MapFunction<String, JSONObject> {

    @Override
    public JSONObject map(String value) {
        try {
            if (value == null || value.trim().isEmpty()) {
                System.out.println("⚠️ EnhancedMapUserImage: 输入为空");
                return null;
            }

            JSONObject obj = JSON.parseObject(value);
            if (obj == null) {
                System.out.println("⚠️ EnhancedMapUserImage: JSON解析失败");
                return null;
            }

            // 统一user_id格式
            String userId = obj.getString("user_id");
            if (userId != null) {
                userId = userId.toLowerCase().trim();
            }
            if (userId == null || userId.isEmpty()) {
                System.out.println("⚠️ EnhancedMapUserImage: user_id 字段缺失");
                return null;
            }

            JSONObject result = new JSONObject();
            result.put("user_id", userId);
            result.put("product_id", obj.getString("product_id"));
            result.put("order_id", obj.getString("order_id"));
            result.put("ts", obj.getLong("ts"));

            // 添加设备信息
            if (obj.containsKey("device")) {
                JSONObject device = obj.getJSONObject("device");
                result.put("device_brand", device.getString("brand"));
                result.put("device_platform", device.getString("plat"));
            }

            // 添加网络信息
            if (obj.containsKey("network")) {
                JSONObject network = obj.getJSONObject("network");
                result.put("network_type", network.getString("net"));
            }

            // 添加操作类型
            result.put("op_type", obj.getString("opa"));
            result.put("log_type", obj.getString("log_type"));
            result.put("source_type", "user_log");

            System.out.println("✅ EnhancedMapUserImage 处理成功: user_id=" + userId);
            return result;
        } catch (Exception e) {
            System.err.println("❌ EnhancedMapUserImage 解析错误: " + e.getMessage());
            return null;
        }
    }
}