package com.star.gmall.realtime.ods

import com.star.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JDouble, JInt, JLong, JString}

import scala.collection.mutable.ListBuffer

abstract class BaseApp {
  val f = org.json4s.DefaultFormats

  var appName:String
  var groupId:String
  var topic:String

  val toLong = new CustomSerializer[Long](ser = format=>({
    case JString(s) => s.toLong
    case JInt(s) => s.toLong
  },
    {case s:Long => JLong(s)})
  )

  val toDouble = new CustomSerializer[Double](ser = format=>({
    case JString(s) => s.toDouble
    case JInt(s) => s.toDouble
  },
    {case s:Double => JDouble(s)})
  )

  def run(ssc:StreamingContext,offsetRanges:ListBuffer[OffsetRange],sourceStream:DStream[ConsumerRecord[String,String]])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10))

    val fromOffsets = OffsetManager.readOffsets(groupId, topic)
    val offsetRanges = ListBuffer.empty[OffsetRange]

    val kafkaDS: DStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(ssc, groupId, topic, fromOffsets)
      .transform(rdd => {
        offsetRanges.clear()
        val newOffsetRagnes = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges ++= newOffsetRagnes
        rdd
      })

    run(ssc,offsetRanges,kafkaDS)

    ssc.start()
    ssc.awaitTermination()


  }

}
