package com.star.gmall.realtime.ods

import com.star.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

abstract class BaseAppV3 {
  val f = org.json4s.DefaultFormats

  var appName:String
  var groupId:String
  var topics:Seq[String]

  def run(ssc:StreamingContext,offsetRanges:ListBuffer[OffsetRange],topicToStreamMap:Map[String,DStream[ConsumerRecord[String,String]]])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10))

    val fromOffsets = OffsetManager.readOffsets(groupId, topics)
    val offsetRanges = ListBuffer.empty[OffsetRange]

    val topicToStreamMap: Map[String, DStream[ConsumerRecord[String, String]]] = topics
        .map(topic=>{
          val ownOffsets = fromOffsets.filter(_._1.topic() == topic)
          val stream = MyKafkaUtil.getKafkaStream(ssc, groupId, topic, ownOffsets)
            .transform(rdd => {
              val newOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
              offsetRanges ++= newOffsetRanges
              rdd
            })
          (topic,stream)
        }).toMap //转map的目的是方便操作，取出需要的流

    run(ssc,offsetRanges,topicToStreamMap)

    ssc.start()
    ssc.awaitTermination()


  }

}
