package com.star.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class BaseLogApp {

    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) {
        try {
//            System.setProperty("HADOOP_USER_NAME","star");
            //1 创建执行环境
            StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(3);//设置并行度，与kafka分区数一致
            //设置检查点
            environment.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
            //设置超时时间
            environment.getCheckpointConfig().setCheckpointTimeout(60000);
            //设置状态后端
//            environment.setStateBackend(new FsStateBackend("hdfs://node:8020/gmall/checkpoint/baselogapp"));
            System.setProperty("HADOOP_USER_NAME", "star");
            //2 add source

            DataStreamSource<String> kafkaSource = environment.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "con_log_group"));
            //3 transform
            SingleOutputStreamOperator<JSONObject> JsonDS = kafkaSource.map(str -> {
                JSONObject jsonObject = JSON.parseObject(str);
                return jsonObject;
            });

            KeyedStream<JSONObject, Object> keybyDS = JsonDS.keyBy(json ->
                    json.getJSONObject("common").getString("mid")
            );

            SingleOutputStreamOperator<JSONObject> mapDsWithFlag = keybyDS.map(new RichMapFunction<JSONObject, JSONObject>() {
                private ValueState<String> firstVisitDateState;

                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    Long ts = value.getLong("ts");
                    String is_new = value.getJSONObject("common").getString("is_new");
                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());

                    String tsDate = localDateTime.toLocalDate().toString().replace("-", "");
                    String stateDate = firstVisitDateState.value();

                    System.out.println("ts:" + ts + "\ttsDate:" + tsDate);
                    if ("1".equals(is_new)) {
                        if (stateDate != null && !"".equals(stateDate)) {
                            if (tsDate.compareTo(stateDate) > 0) {
                                value.getJSONObject("common").put("is_new", "0");
                            }
                        } else {
                            firstVisitDateState.update(tsDate);
                        }
                    }
                    return value;
                }

                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
                }
            });


            final OutputTag<String> startTag = new OutputTag<String>("side-output-start") {
            };
            final OutputTag<String> displayTag = new OutputTag<String>("side-output-display") {
            };

            SingleOutputStreamOperator<String> processDS = mapDsWithFlag.process(new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                    JSONObject start = value.getJSONObject("start");
                    //start log
                    if (start != null && !"".equals(start)) {
                        ctx.output(startTag, start.toString());
                    } else {
                        //日志分为两类，启动和非启动，非启动包含曝光日志
                        out.collect(value.toString());
                        //no start log
                        JSONArray displays = value.getJSONArray("displays");

                        if (displays != null && displays.size() > 0) {
                            String page_id = value.getJSONObject("page").getString("page_id");
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.put("page_id", page_id);
                                ctx.output(displayTag, display.toString());
                            }
                        }
                    }
                }
            });

            DataStream<String> startDS = processDS.getSideOutput(startTag);
            DataStream<String> displayDS = processDS.getSideOutput(displayTag);

            processDS.print("main process=>>>>>>");
            startDS.print("startDS process=>>>>>>");
            displayDS.print("displayDS process=>>>>>>");
            //4 sink
            processDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));
            startDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
            displayDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));

            //5 execute
            environment.execute("ur name");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
