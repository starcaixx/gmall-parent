package com.star.gmall.realtime.app.dws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import com.star.gmall.realtime.bean.VisitorStats;
import com.star.gmall.realtime.utils.DateTimeUtil;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import scala.Tuple4;

import java.time.Duration;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度
        env.setParallelism(3);

        //设置checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/visitorstat/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        String groupId = "visitor_stat_app";
//        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageViewSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

//        度量包括PV、UV、跳出次数、进入页面数(session_count)、连续访问时长
//        维度包括在分析中比较重要的几个字段：渠道、地区、版本、新老用户进行聚
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParanamerModule());

        //转换pv流
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDS = pageViewSourceDS.map(str -> {
            JsonNode jsonNode = objectMapper.readValue(str, JsonNode.class);
            JsonNode common = jsonNode.get("common");
            return new VisitorStats("", "", common.get("vc").asText(),
                    common.get("ch").asText(),
                    common.get("ar").asText(),
                    common.get("is_new").asText(),
                    0l, 1l, 0l, 0l,
                    jsonNode.get("page").get("during_time").asLong(),
                    jsonNode.get("ts").asLong()
            );
        });

        pageViewStatsDS.print("pageViewStatsDS>>>>>>>>>");

        //转换uv
        SingleOutputStreamOperator<VisitorStats> uniqueVistatsDS = uniqueVisitSourceDS.map(str -> {
            JsonNode jsonNode = objectMapper.readValue(str, JsonNode.class);
            JsonNode common = jsonNode.get("common");
            return new VisitorStats("", "",
                    common.get("vc").asText(),
                    common.get("ch").asText(),
                    common.get("ar").asText(),
                    common.get("is_new").asText(),
                    1l, 0l, 0l, 0l, 0l,
                    jsonNode.get("ts").asLong()
            );
        });
        uniqueVistatsDS.print("uniqueVistatsDS>>>>>>>>>");
        //转换sv
        SingleOutputStreamOperator<VisitorStats> sessionVisitDS = pageViewSourceDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<VisitorStats> out) throws Exception {
                JsonNode jsonNode = objectMapper.readValue(value, JsonNode.class);
                String lastPageId = jsonNode.get("page").get("last_page_id").asText();
                if (lastPageId == null || "".equals(lastPageId)) {
                    JsonNode common = jsonNode.get("common");
                    VisitorStats visitorStats = new VisitorStats("", "",
                            common.get("vc").asText(),
                            common.get("ch").asText(),
                            common.get("ar").asText(),
                            common.get("is_new").asText(),
                            0l, 0l, 1l, 0l, 0l,
                            jsonNode.get("ts").asLong()
                    );
                    out.collect(visitorStats);
                }
            }
        });

        sessionVisitDS.print("sessionVisitDS>>>>>>>>>");
        SingleOutputStreamOperator<VisitorStats> userJumpStatDS = userJumpDetailDS.map(str -> {
            JsonNode jsonNode = objectMapper.readValue(str, JsonNode.class);
            JsonNode common = jsonNode.get("common");
            return new VisitorStats("", "",
                    common.get("vc").asText(),
                    common.get("ch").asText(),
                    common.get("ar").asText(),
                    common.get("is_new").asText(),
                    0l, 0l, 0l, 1l, 0l,
                    jsonNode.get("ts").asLong()
            );
        });

        userJumpStatDS.print("userJumpStatDS>>>>>>>>>");

        DataStream<VisitorStats> unionDS = pageViewStatsDS.union(uniqueVistatsDS, sessionVisitDS, userJumpStatDS);

        //watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((visit, ts) -> visit.getTs()));


        visitorStatWithWMDS.print("visitorStatWithWMDS>>>>>>>>");

        //分组 选取四个维度作为key , 使用Tuple4组合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatKeyedbyDS = visitorStatWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        });

        visitorStatKeyedbyDS.print("visitorStatKeyedbyDS>>>>>>");

        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = visitorStatKeyedbyDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        //reduce聚合统计
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                for (VisitorStats element : elements) {
                    long start = context.window().getStart();
                    long end = context.window().getEnd();
                    element.setStt(DateTimeUtil.ts2Str(start));
                    element.setEdt(DateTimeUtil.ts2Str(end));
                    out.collect(element);
                }
            }
        });

        reduceDS.print("reduceDS>>>>>>>>>");

        env.execute();
    }
}
