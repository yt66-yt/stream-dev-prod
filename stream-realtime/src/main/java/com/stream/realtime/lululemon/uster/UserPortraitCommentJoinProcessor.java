package com.stream.realtime.lululemon.uster;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class UserPortraitCommentJoinProcessor {

    /**
     * 简单 Left Join: user logs + comment by user_id
     * 修改方法签名以支持 DataStream 输入
     */
    public static DataStream<JSONObject> process(
            DataStream<JSONObject> userStream,
            DataStream<JSONObject> commentStream
    ) {
        return userStream
                .connect(commentStream)
                .flatMap(new UserPortraitCommentJoiner());
    }
}