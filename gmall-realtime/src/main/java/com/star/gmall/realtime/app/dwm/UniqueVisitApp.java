package com.star.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/uvapp/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "con_page_group"));

//        {"common":{"ar":"110000","uid":"14","os":"iOS 13.3.1","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_18","vc":"v2.0.1","ba":"iPhone"},"page":{"page_id":"comment","item":"9","during_time":17908,"item_type":"sku_id","last_page_id":"good_spec","source_type":"activity"},"ts":1634386047000}

//        {"common":{"ar":"110000","uid":"14","os":"iOS 13.3.1","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_18","vc":"v2.0.1","ba":"iPhone"},"page":{"page_id":"comment","item":"9","during_time":17908,"item_type":"sku_id","last_page_id":"good_spec","source_type":"activity"},"ts":1634386047000}

        SingleOutputStreamOperator<JSONObject> mapDS = kafkaDS.map(str -> JSON.parseObject(str));

        KeyedStream<JSONObject, String> keybyDS = mapDS.keyBy(json->json.getJSONObject("common").getString("mid"));

//        keybyDS.print("key>>>>>>>");

        SingleOutputStreamOperator<JSONObject> filterDS = keybyDS.filter(new RichFilterFunction<JSONObject>() {
            private transient ValueState<String> uids;
            private DateTimeFormatter formatter;

            @Override
            public void open(Configuration parameters) throws Exception {
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .disableCleanupInBackground() //禁止清理状态后端
//                                .cleanupFullSnapshot() //不适用于 RocksDB 状态后端的增量检查点
//                                .cleanupIncrementally(10,true) //默认(5,false) 对RocksDB不适用
//                                .cleanupInRocksdbCompactFilter(1000)//压缩过滤器进行后台清理
                        .build();
                ValueStateDescriptor<String> uidDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);
                uids = getRuntimeContext().getState(uidDescriptor);
                uidDescriptor.enableTimeToLive(ttlConfig);
                super.open(parameters);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                Long ts = value.getLong("ts");
                LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());

                String logDate = localDateTime.format(formatter);
                String lastViewDate = uids.value();

                if (lastViewDate != null && lastViewDate.length() > 0 && logDate.equals(lastViewDate)) {
                    System.out.println("已访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                    uids.update(logDate);
                    return true;
                }
            }
        });

        SingleOutputStreamOperator<String> resultDS = filterDS.map(jsonObj -> jsonObj.toString());
        resultDS.print("result>>>>>>>>");

        resultDS.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));
        env.execute();
    }
}
