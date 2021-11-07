package com.star.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.app.func.DimAsyncFunction;
import com.star.gmall.realtime.bean.OrderDetail;
import com.star.gmall.realtime.bean.OrderInfo;
import com.star.gmall.realtime.bean.OrderWide;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从1。12开始，默认的时间语义就是事件时间，之前默认是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/orderwide/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
//        env.setRestartStrategy(RestartStrategies.noRestart());

        String orderDetailSourceTopic = "dwd_order_detail";
        String orderInfoSourceTopic = "dwd_order_info";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> sourceOrderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));
        DataStreamSource<String> sourceOrderInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));

//        sourceOrderDetailDS.print("orderdetail>>>>>>");
//        sourceOrderInfoDS.print("orderinfo>>>>>>>");
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

//        orderInfoDS.print("orderInfo>>>>>>");
//        orderDetailDS.print("orderDetail>>>>");

        //设定事件时间水位，单调递增
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTimestampDS = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().
                withTimestampAssigner((order, timestamp) -> order.getCreate_ts()));

        SingleOutputStreamOperator<OrderDetail> orderDetailInfoWithTimestampDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().
                withTimestampAssigner((order, timestamp) -> order.getCreate_ts()));


        //设定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeybyDS = orderInfoWithTimestampDS.keyBy(orderInfo -> orderInfo.getId());

        KeyedStream<OrderDetail, Long> orderDetailKeybyDS = orderDetailInfoWithTimestampDS.keyBy(orderDetail -> orderDetail.getOrder_id());

        //订单和明细表关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeybyDS.intervalJoin(orderDetailKeybyDS).between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        orderWideDS.print("orderWideDS>>>>>>>>>>");

        //关联用户纬表
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

                String birthday = jsonObject.getString("BIRTHDAY");
                LocalDate localDate = LocalDate.parse(birthday, formatter);

                long curTs = System.currentTimeMillis();
                long birthdayTs = localDate.atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
                long birthdayTs1 = localDate.atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
                System.out.println("birthdayTs:"+birthdayTs+"::birthdayTs1:"+birthdayTs1);
                long betweenMs = curTs - birthdayTs;

                Long ageLong = betweenMs / 1000l / 60l / 60l / 24l / 365l;
                orderWide.setUser_age(ageLong.intValue());
                orderWide.setUser_gender(jsonObject.getString("GENDER"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getUser_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithUserDS.print("dim join user>>>>>>>");

        //关联省市纬度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setProvince_name(jsonObject.getString("NAME"));
                orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getProvince_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithProvinceDS.print("orderWideWithProvinceDS>>>>>>>>>");
        //SKU纬度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                orderWide.setTm_id(jsonObject.getLong("TM_ID"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getSku_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithSKUDS.print("orderWideWithSKUDS>>>>>>>>");
        //关联spu纬度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSKUDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getSpu_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithSpuDS.print("orderWideWithSpuDS>>>>>>>>>");
        //品类纬度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {

            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setCategory3_name(jsonObject.getString("NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getCategory3_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithCategoryDS.print("orderWideWithCategoryDS>>>>>>>>>");
        //品牌纬度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithCategoryDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getTm_id());
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithTmDS.print("orderWideWithTmDS>>>>>>>>>>");
        orderWideWithTmDS.map(orderWide -> JSON.toJSONString(orderWide)).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
        env.execute();
    }
}
