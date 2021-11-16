package com.star.gmall.realtime.app.dwm;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import com.star.gmall.realtime.bean.OrderWide;
import com.star.gmall.realtime.bean.PaymentInfo;
import com.star.gmall.realtime.bean.PaymentWide;
import com.star.gmall.realtime.utils.DateTimeUtil;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:8020/gmall/paywide/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentInfoSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParanamerModule());

//        paymentInfoSourceDS.print("paymentInfoSourceDS>>>>>>>>>>");
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoSourceDS.map(str -> {
            return objectMapper.readValue(str, PaymentInfo.class);
//            return objectMapper.readValue(str, JsonNode.class);
        });

        orderWideSourceDS.print("orderWideSourceDS>>>>>>>>>>>");
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideSourceDS.map(str -> {
            return objectMapper.readValue(str, OrderWide.class);
        });

        //设定事件时间水位
        SingleOutputStreamOperator<OrderWide> orderWideWithWMDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, timestamp) -> DateTimeUtil.str2Ts(order.getCreate_time())));

        //设定事件时间水位
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWMDS = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((payment, timestamp) -> DateTimeUtil.str2Ts(payment.getCreate_time())));

        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWMDS.keyBy(OrderWide::getOrder_id);
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWMDS.keyBy(PaymentInfo::getOrder_id);

        /*
        keyedStream.intervalJoin(otherKeyedStream)
.between(Time.milliseconds(-2), Time.milliseconds(2)) // lower and upper bound
.upperBoundExclusive(true) // optional
.lowerBoundExclusive(true) // optional
.process(new IntervalJoinFunction() {...});
         */

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                }).uid("payment_join");

        SingleOutputStreamOperator<String> resultDS = paymentWideDS.map(payment -> {
            return objectMapper.writeValueAsString(payment);
        });

        resultDS.print("pay:");
        resultDS.addSink(
                MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}
