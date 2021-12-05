package com.star.gmall.realtime.app

import java.time.LocalDate

import com.star.gmall.realtime.bean.StartupLog
import com.star.gmall.realtime.util.{EsUtils, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

object DauApp {

  val groupId = "bigdata"
  val topic = "ods_base_log"
  def parseToStartupLog(kafkaDS: DStream[String]) = {
    /*
    {
	"common": {
		"ar": "500000",
		"ba": "Honor",
		"ch": "wandoujia",
		"is_new": "1",
		"md": "Honor 20s",
		"mid": "mid_6",
		"os": "Android 11.0",
		"uid": "15",
		"vc": "v2.1.132"
	},
	"page": {
		"during_time": 5466,
		"item": "7,9",
		"item_type": "sku_ids",
		"last_page_id": "trade",
		"page_id": "payment"
	},
	"ts": 1638674441000
}
     */
    kafkaDS.map(jsonStr=>{
      val j: JValue = JsonMethods.parse(jsonStr)
      val jCommon = j \ "common"
      val jTs = j \ "ts"
      implicit val d = org.json4s.DefaultFormats
      jCommon.merge(JObject("ts" -> jTs)).extract[StartupLog]
    })
  }

  def distinct(startupLogDS: DStream[StartupLog]) = {
    val preKey = "mids:"

    startupLogDS.mapPartitions(startupLogs=>{
      val client = RedisUtil.getClient
      val logs = startupLogs.filter(startupLog => {
        val key = preKey + startupLog.logDate
        val result = client.sadd(key, startupLog.mid)

        result == 1
      })
      client.close()
      logs
    })

    /*startupLogDS.filter(startupLog =>{
      val client = RedisUtil.getClient
      val key = preKey + startupLog.logDate
      //把mid写入到Set中，如果返回值为1表示写入成功，返回值为0写入失败
      val result = client.sadd(key, startupLog.mid)
      client.close()
      print(result)
      print(result==1)
      result == 1
    })*/
  }

  def saveToEs(noDupDS: DStream[StartupLog],offsetRanges: ListBuffer[OffsetRange]) = {
    noDupDS.foreachRDD(rdd=>{
      //driver
      rdd.foreachPartition(startupLogs=>{
        //executor
        val today = LocalDate.now().toString
        EsUtils.insertBulk(s"gmall_dau_info_$today",startupLogs)
      })
      OffsetManager.saveOffsets(offsetRanges,groupId,topic)
    })
  }


  def main(args: Array[String]): Unit = {
    //1 创建一个streamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))
    //2 获取启动日志
//    val kafkaDS = MyKafkaUtil.getKafkaStream(ssc, "ods_base_log")

    val fromOffsets: Map[TopicPartition, Long] = OffsetManager.readOffsets(groupId,topic)

    val offsetRanges = ListBuffer.empty[OffsetRange]
    val kafkaDS = MyKafkaUtil.getKafkaStream(ssc, groupId, topic, fromOffsets)
      .transform(rdd=>{
        //driver
        offsetRanges.clear()
        val newOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges ++= newOffsetRanges
        rdd
      })

    val startupLogDS = parseToStartupLog(kafkaDS.map(_.value()))
    startupLogDS.print()

    val noDupDS = distinct(startupLogDS)

    saveToEs(noDupDS,offsetRanges)

    ssc.start()


    ssc.awaitTermination()
  }



}
