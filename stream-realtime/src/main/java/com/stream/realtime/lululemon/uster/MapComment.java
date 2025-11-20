package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 处理评论流 realtime_v3_order_comment
 */
public class MapComment implements MapFunction<String, JSONObject> {

    @Override
    public JSONObject map(String value) throws Exception {
        System.out.println("🔍 MapComment 收到原始数据: " + value);

        try {
            if (value == null || value.trim().isEmpty()) {
                System.out.println("⚠️ MapComment: 输入为空");
                return null;
            }

            JSONObject obj = JSON.parseObject(value);
            if (obj == null) {
                System.out.println("⚠️ MapComment: JSON解析失败");
                return null;
            }

            // 检查必要字段并统一转为小写
            String userId = obj.getString("user_id");
            if (userId != null) {
                userId = userId.toLowerCase(); // 统一转为小写
            }
            if (userId == null || userId.trim().isEmpty()) {
                System.out.println("⚠️ MapComment: user_id 字段缺失或为空");
                return null;
            }

            JSONObject result = new JSONObject();
            result.put("user_id", userId);
            result.put("order_id", obj.getString("orderid"));
            result.put("product_id", "unknown");
            result.put("product_name", "unknown");

            // comment 字段
            if (obj.containsKey("comment")) {
                String commentText = obj.getString("comment");
                result.put("comment", commentText);
            }

            // 评论级别和敏感词信息
            if (obj.containsKey("commentLevel")) {
                result.put("comment_level", obj.getString("commentLevel"));
            }
            if (obj.containsKey("isBlack")) {
                result.put("is_black", obj.getInteger("isBlack"));
            }
            if (obj.containsKey("sensitiveWords")) {
                result.put("sensitive_words", obj.getJSONArray("sensitiveWords"));
            }

            // 时间戳
            Long timestamp = obj.getLong("ts");
            if (timestamp == null) {
                timestamp = System.currentTimeMillis();
            }
            result.put("ts", timestamp);

            System.out.println("✅ MapComment 处理成功: user_id=" + userId);

            return result;

        } catch (Exception e) {
            System.err.println("❌ MapComment 解析错误: " + e.getMessage() + ", 数据: " + value);
            e.printStackTrace();
            return null;
        }
    }
}