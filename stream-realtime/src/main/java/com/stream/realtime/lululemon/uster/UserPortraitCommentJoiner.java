package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 以 user_id 为 key 的简单 state join
 */
public class UserPortraitCommentJoiner implements CoFlatMapFunction<JSONObject, JSONObject, JSONObject> {

    private final Map<String, JSONObject> userState = new HashMap<>();
    private final Map<String, JSONObject> commentState = new HashMap<>();

    @Override
    public void flatMap1(JSONObject user, Collector<JSONObject> out) {
        try {
            String uid = user.getString("user_id");
            if (uid == null || uid.trim().isEmpty()) {
                return;
            }

            userState.put(uid, user);
            System.out.println("📥 更新用户状态: " + uid + ", 当前用户状态数: " + userState.size());

            // 详细日志：查看当前用户状态和评论状态
            System.out.println("🔍 用户状态详情 - 用户ID: " + uid);
            System.out.println("   用户数据: " + user.toJSONString());
            System.out.println("   是否存在对应评论: " + commentState.containsKey(uid));

            if (commentState.containsKey(uid)) {
                JSONObject comment = commentState.get(uid);
                System.out.println("   评论数据: " + comment.toJSONString());

                JSONObject joined = join(user, comment);
                System.out.println("🎯 用户-评论 Join 成功: " + uid);
                out.collect(joined);

                // 打印最终结果到控制台
                System.out.println("🚀 FINAL JOINED RESULT: " + joined.toJSONString());
            } else {
                System.out.println("❌ 用户 " + uid + " 没有对应的评论数据");
                System.out.println("   当前评论状态中的用户ID: " + commentState.keySet());
            }
        } catch (Exception e) {
            System.err.println("❌ 处理用户数据失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void flatMap2(JSONObject comment, Collector<JSONObject> out) {
        try {
            String uid = comment.getString("user_id");
            if (uid == null || uid.trim().isEmpty()) {
                return;
            }

            commentState.put(uid, comment);
            System.out.println("📥 更新评论状态: " + uid + ", 当前评论状态数: " + commentState.size());

            // 详细日志：查看当前评论状态和用户状态
            System.out.println("🔍 评论状态详情 - 用户ID: " + uid);
            System.out.println("   评论数据: " + comment.toJSONString());
            System.out.println("   是否存在对应用户: " + userState.containsKey(uid));

            if (userState.containsKey(uid)) {
                JSONObject user = userState.get(uid);
                System.out.println("   用户数据: " + user.toJSONString());

                JSONObject joined = join(user, comment);
                System.out.println("🎯 评论-用户 Join 成功: " + uid);
                out.collect(joined);

                // 打印最终结果到控制台
                System.out.println("🚀 FINAL JOINED RESULT: " + joined.toJSONString());
            } else {
                System.out.println("❌ 评论 " + uid + " 没有对应的用户数据");
                System.out.println("   当前用户状态中的用户ID: " + userState.keySet());
            }
        } catch (Exception e) {
            System.err.println("❌ 处理评论数据失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private JSONObject join(JSONObject user, JSONObject comment) {
        JSONObject obj = new JSONObject();

        // 用户信息
        obj.put("user_id", user.getString("user_id"));
        obj.put("user_product_id", user.getString("product_id"));
        obj.put("user_order_id", user.getString("order_id"));
        obj.put("ts_user", user.getLong("ts"));

        // 评论信息
        obj.put("comment_order_id", comment.getString("order_id"));
        obj.put("comment_text", comment.getString("comment"));
        obj.put("ts_comment", comment.getLong("ts"));

        // 评论分析信息
        if (comment.containsKey("comment_level")) {
            obj.put("comment_level", comment.getString("comment_level"));
        }
        if (comment.containsKey("is_black")) {
            obj.put("is_black", comment.getInteger("is_black"));
        }
        if (comment.containsKey("sensitive_words")) {
            obj.put("sensitive_words", comment.getJSONArray("sensitive_words"));
        }

        // 产品信息
        obj.put("matched_product_id", user.getString("product_id"));

        // 用户基础信息 - 优先使用用户流中的信息，如果没有则使用评论流中的
        addUserBaseInfo(obj, user, comment);

        // 添加处理时间戳和匹配信息
        obj.put("process_ts", System.currentTimeMillis());
        obj.put("join_type", "user_portrait_comment_with_postgresql");

        System.out.println("🎉 FINAL JOIN 结果: " + obj.toJSONString());

        return obj;
    }

    private void addUserBaseInfo(JSONObject result, JSONObject user, JSONObject comment) {
        // 优先从用户流获取基础信息，如果没有则从评论流获取
        JSONObject source = user.containsKey("uname") ? user : comment;

        if (source.containsKey("uname")) {
            result.put("uname", source.getString("uname"));
        }
        if (source.containsKey("phone_num")) {
            result.put("phone_num", source.getString("phone_num"));
        }
        if (source.containsKey("birthday")) {
            result.put("birthday", source.getString("birthday"));
        }
        if (source.containsKey("gender")) {
            result.put("gender", source.getString("gender"));
        }
        if (source.containsKey("address")) {
            result.put("address", source.getString("address"));
        }
        if (source.containsKey("user_info_ts")) {
            result.put("user_info_ts", source.getString("user_info_ts"));
        }
    }


}