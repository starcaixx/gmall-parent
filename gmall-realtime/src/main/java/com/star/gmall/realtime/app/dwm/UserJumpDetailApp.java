package com.star.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:8020/gmall/uvapp/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "con_pages_group"));

        DataStreamSource<String> dataDS = env.fromElements("");

        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(str -> JSON.parseObject(str));

        //指定事件时间
        SingleOutputStreamOperator<JSONObject> jsonDSwith = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getLong("ts")));

        KeyedStream<JSONObject, String> keyedDS = jsonDSwith.keyBy(json -> json.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin").where(new SimpleCondition<JSONObject>() {
            //进入第一个页面
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || "".equals(lastPageId)) {
                    return true;
                } else {
                    return false;
                }
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                if (pageId != null && !"".equals(pageId)) {
                    return true;
                }
                return false;
            }
        }).within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedDS, pattern);

        OutputTag<String> timeout = new OutputTag<>("timeout");
        SingleOutputStreamOperator<String> filtedStream = patternStream.flatSelect(timeout, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                List<JSONObject> objList = map.get("begin");
                for (JSONObject jsonObject : objList) {
                    collector.collect(jsonObject.toString());
                }
            }
        }, new PatternFlatSelectFunction<JSONObject, String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {

            }
        });
        DataStream<String> jumpStream = filtedStream.getSideOutput(timeout);
        jumpStream.print("jump");

        jumpStream.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        env.execute();
    }
}
