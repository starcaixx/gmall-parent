package com.star.gmall.realtime.ods
import com.star.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object BaseDBCanalApp extends BaseApp {
  override var appName: String = "BaseDBCanalApp"
  override var groupId: String = "bigdata1"
  override var topic: String = "gmall_db"

  val tableNames = List(
    "order_info",
    "order_detail",
    "user_info",
    "base_province",
    "base_category3",
    "sku_info",
    "spu_info",
    "base_trademark")

  override def run(ssc: StreamingContext,
                   offsetRanges: ListBuffer[OffsetRange],
                   sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
    sourceStream.flatMap(record=>{
      val jValue = JsonMethods.parse(record.value())
      val data = jValue \ "data"
      implicit val f = org.json4s.DefaultFormats
      val tableName = (jValue \ "table").extract[String]
      val operate = (jValue \ "type").extract[String]
      data.children.map(child=>{
        (tableName,operate,Serialization.write(child))
      })
    }).filter {
      case (tableName,operate,content) =>
        tableNames.contains(tableName) && operate.toLowerCase() != "delete" && content.length>2
    }.foreachRDD(rdd=>{
      rdd.foreachPartition(it =>{
        val producer = MyKafkaUtil.getKafkaProducer()
        it.foreach {
          case (tableName,operate,content)=>
            val topic = s"ods_$tableName"
            if (tableName!="order_info") {
              producer.send(new ProducerRecord[String,String](topic,content))
            }else if (operate.toLowerCase() == "insert") {
              producer.send(new ProducerRecord[String,String](topic,content))
            }
        }
        producer.close()
      })
      OffsetManager.saveOffsets(offsetRanges,groupId,topic)
    })
  }
}
