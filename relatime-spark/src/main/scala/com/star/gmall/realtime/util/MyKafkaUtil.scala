package com.star.gmall.realtime.util

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.mutable

object MyKafkaUtil {
  val kafkaParams = mutable.Map[String, Object](
    "bootstrap.servers" -> ConfigUtil.getProperty("kafka.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ConfigUtil.getProperty("kafka.group"),
    "auto.offset.reset" -> "latest", // 如果有保存 offset, 则从保存位置开始消费, 没有则从latest开始消费
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParams)
      )
      .map(_.value)
  }

  def getKafkaStream(ssc: StreamingContext,groupId:String, topic: String,fromOffsets:Map[TopicPartition,Long]) = {
    /*kafkaParams("enable.auto.commit")=false
    kafkaParams("group.id")=groupId
    KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParams,fromOffsets)
      )
      .map(_.value)*/
    // 把 offset 自动提交设置为 false, 我们需要手动提交offset
    kafkaParams("enable.auto.commit") = (false: java.lang.Boolean)
    kafkaParams("group.id") = groupId
    KafkaUtils
      .createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParams, fromOffsets)
      )
  }

  def getKafkaStream(ssc: StreamingContext,groupId:String, topics: Seq[String],fromOffsets:Map[TopicPartition,Long]) = {
    /*kafkaParams("enable.auto.commit")=false
    kafkaParams("group.id")=groupId
    KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParams,fromOffsets)
      )
      .map(_.value)*/
    // 把 offset 自动提交设置为 false, 我们需要手动提交offset
    kafkaParams("enable.auto.commit") = (false: java.lang.Boolean)
    kafkaParams("group.id") = groupId
    KafkaUtils
      .createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, fromOffsets)
      )
  }

  val producerParams:Map[String,Object] = Map(
  "bootstrap.servers" -> ConfigUtil.getProperty("kafka.servers"),
  "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "enable.idompotence" -> (true: java.lang.Boolean)
  )
  def getKafkaProducer() ={
    import scala.collection.JavaConverters._
    new KafkaProducer[String,String](producerParams.asJava)
  }

}
