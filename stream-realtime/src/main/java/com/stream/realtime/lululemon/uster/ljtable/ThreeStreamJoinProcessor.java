// ThreeStreamJoinProcessor.java
package com.stream.realtime.lululemon.uster.ljtable;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ThreeStreamJoinProcessor extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {

    // 存储用户基本信息
    private transient MapState<String, JSONObject> userInfoState;

    // 存储用户画像日志
    private transient MapState<String, JSONObject> userLogState;

    // 存储评论数据
    private transient MapState<String, JSONObject> commentState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, JSONObject> userInfoDesc =
                new MapStateDescriptor<>("user-info-state", String.class, JSONObject.class);
        MapStateDescriptor<String, JSONObject> userLogDesc =
                new MapStateDescriptor<>("user-log-state", String.class, JSONObject.class);
        MapStateDescriptor<String, JSONObject> commentDesc =
                new MapStateDescriptor<>("comment-state", String.class, JSONObject.class);

        userInfoState = getRuntimeContext().getMapState(userInfoDesc);
        userLogState = getRuntimeContext().getMapState(userLogDesc);
        commentState = getRuntimeContext().getMapState(commentDesc);
    }

    @Override
    public void processElement1(JSONObject userLog, Context ctx, Collector<JSONObject> out) throws Exception {
        String userId = userLog.getString("user_id");
        if (userId == null) return;

        System.out.println("📥 处理用户日志: " + userId);
        userLogState.put(userId, userLog);

        // 尝试关联
        triggerJoin(userId, out);
    }

    @Override
    public void processElement2(JSONObject comment, Context ctx, Collector<JSONObject> out) throws Exception {
        String userId = comment.getString("user_id");
        if (userId == null) return;

        System.out.println("📥 处理评论数据: " + userId);
        commentState.put(userId, comment);

        // 尝试关联
        triggerJoin(userId, out);
    }

    // 处理用户基本信息流
    public void processElement3(JSONObject userInfo, Context ctx, Collector<JSONObject> out) throws Exception {
        String userId = userInfo.getString("user_id");
        if (userId == null) return;

        System.out.println("📥 处理用户基本信息: " + userId);
        userInfoState.put(userId, userInfo);

        // 尝试关联
        triggerJoin(userId, out);
    }

    private void triggerJoin(String userId, Collector<JSONObject> out) throws Exception {
        JSONObject userInfo = userInfoState.get(userId);
        JSONObject userLog = userLogState.get(userId);
        JSONObject comment = commentState.get(userId);

        // 至少需要用户信息和另外任意一个数据源
        if (userInfo == null) {
            return;
        }

        // 生成关联结果
        if (userLog != null) {
            JSONObject joinedWithLog = joinData(userInfo, userLog, comment);
            out.collect(joinedWithLog);
        }

        if (comment != null) {
            JSONObject joinedWithComment = joinData(userInfo, userLog, comment);
            out.collect(joinedWithComment);
        }
    }

    private JSONObject joinData(JSONObject userInfo, JSONObject userLog, JSONObject comment) {
        JSONObject result = new JSONObject();

        // 用户基本信息
        result.put("user_id", userInfo.getString("user_id"));
        result.put("uname", userInfo.getString("uname"));
        result.put("phone_num", userInfo.getString("phone_num"));
        result.put("birthday", userInfo.getString("birthday"));
        result.put("gender", userInfo.getString("gender"));
        result.put("address", userInfo.getString("address"));

        // 用户行为日志
        if (userLog != null) {
            result.put("log_product_id", userLog.getString("product_id"));
            result.put("log_order_id", userLog.getString("order_id"));
            result.put("log_ts", userLog.getLong("ts"));
            result.put("device_brand", userLog.getString("device_brand"));
            result.put("log_type", userLog.getString("log_type"));
        }

        // 评论数据
        if (comment != null) {
            result.put("comment_order_id", comment.getString("orderid"));
            result.put("comment_text", comment.getString("comment"));
            result.put("comment_level", comment.getString("commentLevel"));
            result.put("is_black", comment.getInteger("isBlack"));
            result.put("sensitive_words", comment.getJSONArray("sensitiveWords"));
        }

        // 元数据
        result.put("join_ts", System.currentTimeMillis());
        result.put("has_user_info", userInfo != null);
        result.put("has_user_log", userLog != null);
        result.put("has_comment", comment != null);

        System.out.println("🎉 三流关联成功: " + result.toJSONString());
        return result;
    }
}