package com.star.gmall.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Properties;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String receiveLog(@RequestParam("param") String jsonLog) {
//        System.ouexit.println(log);

        log.info(jsonLog);

        kafkaTemplate.send("ods_base_log", jsonLog);
//        Properties prop = new Properties();
//        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node:9092");
//        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"");
//        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"");
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);
//        kafkaProducer.send(new ProducerRecord<String,String>("topic",jsonLog));

        return "success";
    }

}
