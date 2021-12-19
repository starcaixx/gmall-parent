package com.star.gmall.realtime.dwd

import java.util.Properties

import com.star.gmall.realtime.bean._
import com.star.gmall.realtime.ods.BaseAppV2
import com.star.gmall.realtime.util.OffsetManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
    "ods_base_trademark"
  )

  def saveToPhoenix[T <: Product](rdd: RDD[(String, String)], odsTopic: String, tableName: String, cols: Seq[String])
                                 (implicit mf: scala.reflect.Manifest[T]) = {
    import org.apache.phoenix.spark._
    rdd.filter(_._1==odsTopic)
      .map{
        case (topic,content)=>
          val formats = org.json4s.DefaultFormats
          JsonMethods.parse(content).extract[T](formats,mf)
      }.saveToPhoenix(tableName,
      cols,//顺序和样例类保持一致
      zkUrl = Option("node:2181"))

  }

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
    val spark = SparkSession.builder()
      .config(ssc.sparkContext.getConf).getOrCreate()

    import spark.implicits._

    sourceStream.map(record=>{
      (record.topic(),record.value())
    }).foreachRDD(rdd=>{
      topics.foreach{
        case "ods_user_info" =>

          saveToPhoenix[UserInfo](rdd,
            "ods_user_info",
            "gmall_user_info",
            Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"))

        case "ods_sku_info" =>
          import org.apache.phoenix.spark._
          saveToPhoenix[SkuInfo](rdd,
          "ods_sku_info",
          "gmall_sku_info",
          Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"))
          // 需要和 gmall_spu_info gmall_base_category3  gmall_base_trademark 连接, 然后得到所有字段
          // 使用 spark-sql 完成
          val url = "jdbc:phoenix:node:2181"
          spark.read.jdbc(url, "gmall_sku_info", new Properties()).createOrReplaceTempView("sku")
          spark.read.jdbc(url, "gmall_spu_info", new Properties()).createOrReplaceTempView("spu")
          spark.read.jdbc(url, "gmall_base_category3", new Properties()).createOrReplaceTempView("category3")
          spark.read.jdbc(url, "gmall_base_trademark", new Properties()).createOrReplaceTempView("tm")
          spark.sql(
            """
              |select
              |    sku.id as id,
              |    sku.spu_id spu_id,
              |    sku.price price,
              |    sku.sku_name sku_name,
              |    sku.tm_id  tm_id,
              |    sku.category3_id  category3_id,
              |    sku.create_time  create_time,
              |    category3.name  category3_name,
              |    spu.spu_name  spu_name,
              |    tm.tm_name  tm_name
              |from sku
              |join spu on sku.spu_id=spu.id
              |join category3 on sku.category3_id=category3.id
              |join tm on sku.tm_id=tm.id
              |""".stripMargin)
            .as[SkuInfo]
            .rdd
            .saveToPhoenix(
              "gmall_sku_info",
              Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
              zkUrl = Option("node:2181"))

        case "ods_spu_info" =>

          saveToPhoenix[SpuInfo](rdd,
            "ods_spu_info",
            "gmall_spu_info",
            Seq("ID", "SPU_NAME"))

        case "ods_base_category3" =>

          saveToPhoenix[BaseCategory3](rdd,
            "ods_base_category3",
            "gmall_base_category3",
            Seq("ID", "NAME", "CATEGORY2_ID"))

        case "ods_base_province" =>

          saveToPhoenix[ProvinceInfo](rdd,
            "ods_base_province",
            "gmall_province_info",
            Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"))

        case "ods_base_trademark" =>


          saveToPhoenix[BaseTrademark](rdd,
            "ods_base_trademark",
            "gmall_base_trademark",
            Seq("ID", "TM_NAME"))


        case topic => throw new UnsupportedOperationException(s"不支持消费此 ${topic}")
        /*case "ods_base_province" =>
          import org.apache.phoenix.spark._
          saveToPhoenix[ProvinceInfo](rdd,"ods_base_province","gmall_province_info",Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"))
        case "ods_user_info"=>
          import org.apache.phoenix.spark._

          saveToPhoenix[UserInfo](rdd,
          "ods_user_info",
          "gmall_user_info",
            Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"))*/
      }

      OffsetManager.saveOffsets(offsetRanges,groupId,topics)
    })
  }
}
