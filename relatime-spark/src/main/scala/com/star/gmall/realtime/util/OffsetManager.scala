package com.star.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object OffsetManager {

  def readOffsets(groupId:String,topic:String):Map[TopicPartition,Long]={
    val client = RedisUtil.getClient
    val partitionToOffsetMap = client.hgetAll(s"offset$groupId:$topic")
    client.close()
    import scala.collection.JavaConverters._
    partitionToOffsetMap.asScala
      .map{
        case (partition,offset)=>
          new TopicPartition(topic,partition.toInt)->offset.toLong
      }.toMap
  }

  def readOffsets(groupId:String,topics:Seq[String]):Map[TopicPartition,Long]={

    import scala.collection.JavaConverters._
    val client = RedisUtil.getClient
    val topicParitionAndOffset = client.hgetAll(s"offset:$groupId:$topics")
      .asScala
      .flatMap{
        case (topic, partitionAndOffset: String) =>
          implicit val f = org.json4s.DefaultFormats
          JsonMethods.parse(partitionAndOffset)
            .extract[Map[Int, Long]]
            .map {
              case (partition, offset) =>
                new TopicPartition(topic, partition) -> offset
          }
      }.toMap
    client.close()
    topicParitionAndOffset
  }

  /**
   * key:offset:xxxApp
   * value:hash field value
   * @param offsetRanges
   * @param groupId
   * @param topics
   */
  def saveOffsets(offsetRanges:ListBuffer[OffsetRange],groupId:String,topics:Seq[String])={
    import scala.collection.JavaConverters._

    val key = s"offset:$groupId"
    val fieldAndValue = offsetRanges.groupBy(_.topic)
      .map {
        case (field, it) =>
          implicit val f = org.json4s.DefaultFormats
          val value = it.map(offsetRange => (offsetRange.partition, offsetRange.untilOffset))
            .toMap
          (field, Serialization.write(value))
      }.asJava

    val client = RedisUtil.getClient
    println("topic->offset:"+fieldAndValue)
    client.hmset(key,fieldAndValue)
    client.close()
  }

  def saveOffsets(offsetRanges:ListBuffer[OffsetRange],groupId:String,topic:String)={
    import scala.collection.JavaConverters._
    val fieldToValue = offsetRanges.map(offsetRange => {
      offsetRange.partition.toString -> offsetRange.untilOffset.toString
    }).toMap
      .asJava

    val client = RedisUtil.getClient
    println("topic->offset:"+fieldToValue)
    client.hmset(s"offset:$groupId:$topic",fieldToValue)
    client.close()
  }

}
