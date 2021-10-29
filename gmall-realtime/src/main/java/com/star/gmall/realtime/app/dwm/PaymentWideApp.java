package com.star.gmall.realtime.app.dwm;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/paywide/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        String orderDetailSourceTopic = "dwd_order_info";
        String orderInfoSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";


        env.execute();
    }
}
