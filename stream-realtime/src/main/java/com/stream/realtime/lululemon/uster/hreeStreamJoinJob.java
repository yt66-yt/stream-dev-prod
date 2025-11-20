package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * 用户画像 + 评论/行为日志 Join（基于 product_id ）
 */
public class hreeStreamJoinJob {

    // Set<String> 需要 Flink 的 TypeHint
    private static final MapStateDescriptor<String, Set<String>> PRODUCT_TO_USERIDS =
            new MapStateDescriptor<>(
                    "product-to-userids",
                    TypeInformation.of(String.class),
                    TypeInformation.of(new TypeHint<Set<String>>() {
                    })
            );

    private static final MapStateDescriptor<String, JSONObject> USERID_TO_PORTRAIT =
            new MapStateDescriptor<>(
                    "userid-to-portrait",
                    TypeInformation.of(String.class),
                    TypeInformation.of(JSONObject.class)
            );

    public static DataStream<JSONObject> process(
            SingleOutputStreamOperator<JSONObject> portraitStream,
            SingleOutputStreamOperator<JSONObject> eventStream) {

        BroadcastStream<JSONObject> broadcastPortrait = portraitStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofMinutes(60))
                                .withTimestampAssigner((e, ts) -> e.getLongValue("ts") * 1000L)
                )
                .broadcast(PRODUCT_TO_USERIDS, USERID_TO_PORTRAIT);

        return eventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofMinutes(60))
                                .withTimestampAssigner((e, ts) -> e.getLongValue("ts") * 1000L)
                )
                .connect(broadcastPortrait)
                .process(new BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>() {

                    @Override
                    public void processBroadcastElement(JSONObject portrait,
                                                        Context ctx,
                                                        Collector<JSONObject> out) throws Exception {

                        BroadcastState<String, Set<String>> productState = ctx.getBroadcastState(PRODUCT_TO_USERIDS);
                        BroadcastState<String, JSONObject> portraitState = ctx.getBroadcastState(USERID_TO_PORTRAIT);

                        String userid = portrait.getString("userid");
                        if (userid == null || userid.trim().isEmpty()) {
                            return;
                        }

                        // 保存画像
                        portraitState.put(userid, portrait);

                        Set<String> productIds = new HashSet<>();

                        // 多商品数组
                        if (portrait.containsKey("product_info")) {
                            JSONArray arr = portrait.getJSONObject("product_info").getJSONArray("product_ids");
                            if (arr != null) {
                                for (int i = 0; i < arr.size(); i++) {
                                    String clean = arr.getString(i).replace("\n", "").trim();
                                    if (!clean.isEmpty()) {
                                        productIds.add(clean);
                                    }
                                }
                            }
                        }

                        // 单个 product_id
                        String pid2 = portrait.getString("product_id");
                        if (pid2 != null && !pid2.trim().isEmpty()) {
                            productIds.add(pid2.trim());
                        }

                        // 更新广播 product→userids
                        for (String pid : productIds) {
                            Set<String> set = productState.get(pid);
                            if (set == null) {
                                set = new HashSet<>();
                            }
                            set.add(userid);
                            productState.put(pid, set);
                        }
                    }

                    @Override
                    public void processElement(JSONObject event,
                                               ReadOnlyContext ctx,
                                               Collector<JSONObject> out) throws Exception {

                        String productId = event.getString("product_id");
                        if (productId == null) {
                            return;
                        }
                        productId = productId.trim();

                        ReadOnlyBroadcastState<String, Set<String>> productState = ctx.getBroadcastState(PRODUCT_TO_USERIDS);
                        ReadOnlyBroadcastState<String, JSONObject> portraitState = ctx.getBroadcastState(USERID_TO_PORTRAIT);

                        Set<String> userids = productState.get(productId);
                        if (userids == null || userids.isEmpty()) {
                            return;
                        }

                        for (String uid : userids) {
                            JSONObject portrait = portraitState.get(uid);
                            if (portrait == null) {
                                continue;
                            }

                            JSONObject merged = new JSONObject();
                            merged.putAll(portrait);
                            merged.putAll(event);

                            enrichWithMetadata(merged, uid, productId);
                            out.collect(merged);
                        }
                    }
                })
                .name("portrait-event-product-join");
    }

    private static void enrichWithMetadata(JSONObject result, String uid, String pid) {
        result.put("matched_by", "product_id");
        result.put("matched_userid", uid);
        result.put("matched_product_id", pid);

        double totalAmount = result.getDoubleValue("total_amount");
        if (totalAmount > 0) {
            if (totalAmount <= 2000) {
                result.put("consumption_level", "low");
            } else if (totalAmount <= 5000) {
                result.put("consumption_level", "mid");
            } else {
                result.put("consumption_level", "high");
            }
        }
    }
}
