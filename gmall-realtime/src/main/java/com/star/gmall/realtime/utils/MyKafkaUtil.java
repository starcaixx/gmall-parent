package com.star.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static final  String DEFAULT_TOPIC="DEFAULT_DATA";
    private static final String kafkaServer = "node:9092";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(kafkaServer,topic,new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkbySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties prop =new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        //如果15分钟没有更新状态，则超时 默认1分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");
        return new FlinkKafkaProducer<>(DEFAULT_TOPIC,serializationSchema,prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
