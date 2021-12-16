package com.star.gmall.realtime.ods
import com.star.gmall.realtime.bean.{ProvinceInfo, UserInfo}
import com.star.gmall.realtime.util.OffsetManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

object DwdDimApp extends BaseAppV2 {
  override var appName: String = "DwdDimApp"
  override var groupId: String = "DwdDimApp"
  override var topics: Seq[String] = Seq(
    "ods_user_info",
    "ods_sku_info",
    "ods_spu_info",
    "ods_base_category3",
    "ods_base_province",
    "ods_base_trademark")

  def saveToPhoenix[T <: Product](rdd: RDD[(String, String)], odsTopic: String, tableName: String, cols: Seq[String]) = {
    import org.apache.phoenix.spark._
    rdd.filter(_._1==odsTopic)
      .map{
        case (topic,content)=>
          implicit val f = org.json4s.DefaultFormats
          JsonMethods.parse(content).extract[T]
      }.saveToPhoenix(tableName,
      cols,//顺序和样例类保持一致
      zkUrl = Option("node:2181"))

  }

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
    sourceStream.map(record=>{
      (record.topic(),record.value())
//      implicit val f = org.json4s.DefaultFormats
//      JsonMethods.parse((record.topic(),record.value()))
//      JsonMethods.parse(record.value()).extract[ProvinceInfo]
    }).foreachRDD(rdd=>{
      topics.foreach{
        case "ods_base_province" =>
          print("")
        case "ods_user_info"=>
          import org.apache.phoenix.spark._

          saveToPhoenix[UserInfo](rdd,
          "ods_user_info",
          "gmall_user_info",
            Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"))
      }
    })
    /*.foreachRDD(rdd=>{
      case "ods_base_province" =>
        import org.apache.phoenix.spark._
        print("ss")
        rdd.saveToPhoenix("gmall_province_info",Seq("ID","NAME","AREA_CODE","ISO_CODE"),zkUrl=Option("node:2181"))
      case "ods_user_info" =>
        rdd.filter()
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix("gmall_province_info",Seq("ID","NAME","AREA_CODE","ISO_CODE"),zkUrl=Option("node:2181"))

      OffsetManager.saveOffsets(offsetRanges,groupId,topics)
    })*/

  }
}
