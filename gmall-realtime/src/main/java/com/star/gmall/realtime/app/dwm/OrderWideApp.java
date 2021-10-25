package com.star.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.bean.OrderDetail;
import com.star.gmall.realtime.bean.OrderInfo;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/uvapp/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        String orderDetailSourceTopic = "dwd_order_info";
        String orderInfoSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> sourceOrderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));
        DataStreamSource<String> sourceOrderInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));

        sourceOrderDetailDS.print("orderdetail>>>>>>");
        sourceOrderInfoDS.print("orderinfo>>>>>>>");
        //转换结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = sourceOrderDetailDS.map(str -> {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            OrderDetail orderDetail = JSON.parseObject(str, OrderDetail.class);
            LocalDateTime parse = LocalDateTime.parse(orderDetail.getCreate_time(), dtf);
//日期转时间戳
            orderDetail.setCreate_ts(LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
            return orderDetail;
        });

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = sourceOrderInfoDS.map(str -> {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            OrderInfo orderInfo = JSON.parseObject(str, OrderInfo.class);
            LocalDateTime parse = LocalDateTime.parse(orderInfo.getCreate_time(), dtf);
//日期转时间戳
            orderInfo.setCreate_ts(LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
            return orderInfo;
        });

        orderInfoDS.print("orderInfo>>>>>>");
        orderDetailDS.print("orderDetail>>>>");

        env.execute();
    }
}
