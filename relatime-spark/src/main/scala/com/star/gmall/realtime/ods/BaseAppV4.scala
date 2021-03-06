package com.star.gmall.realtime.ods

import com.star.gmall.realtime.util.{MyKafkaUtil, OffsetManagerV2}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

abstract class BaseAppV4 {
  var appName: String
  var groupId: String
  var topic: String

  def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]])

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val fromOffsets: Map[TopicPartition, Long] = OffsetManagerV2.readOffset(groupId, topic)
    val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]

    val sourceStream: DStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(ssc, groupId, topic, fromOffsets)
      .transform(rdd => {
        offsetRanges.clear
        val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges ++= newOffsetRanges
        rdd
      })

    run(ssc, offsetRanges, sourceStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
