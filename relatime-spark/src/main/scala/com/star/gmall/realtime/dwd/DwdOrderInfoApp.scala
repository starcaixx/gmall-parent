package com.star.gmall.realtime.dwd

import java.time.LocalDate

import com.star.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.star.gmall.realtime.ods.BaseApp
import com.star.gmall.realtime.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object DwdOrderInfoApp extends BaseApp {
  override var appName: String = "DwdOrderInfoApp"
  override var groupId: String = "DwdOrderInfoApp"
  override var topic: String = "ods_order_info"

  implicit val format = f
  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {

    val value = sourceStream.map(record => {
      JsonMethods.parse(record.value()).extract[OrderInfo]
    })
    val firstOrderInfoDS = value.mapPartitions(orders => {
        val orderInfoList = orders.toList
        val userIds = orderInfoList.map(_.user_id).mkString("','")
        //根据该批次的user_id查询是否是首单
        val userIdToConsumed = PhoenixUtil.query(s"select user_id,is_consumed from user_status where user_id in ('${userIds}')",
          Nil).map(map => {
          map("user_id").toString -> map("is_consumed").asInstanceOf[Boolean]
        }).toMap
        //反向判断是否是首单
        orderInfoList.map(order => {
          order.is_first_order = !userIdToConsumed.contains(order.user_id.toString)
          order
        }).toIterator
    })

    val resultDS = firstOrderInfoDS.map(order => (order.user_id, order))
      .groupByKey()
      .flatMap {
        case (user_id, orders) =>
          val orderInfoList = orders.toList
          if (orderInfoList.head.is_first_order) {
            val sortedOrderInfoList = orderInfoList.sortBy(_.create_time)
            orderInfoList.head :: orderInfoList.tail.map(info => {
              info.is_first_order = false
              info
            })
          } else orders
      }

    val spark = SparkSession.builder()
      .config(ssc.sparkContext.getConf).getOrCreate()

    import spark.implicits._

    resultDS.transform(rdd=>{
      rdd.cache()
      val provinceIds = rdd.map(_.province_id).collect().mkString("'", "','", "'")
      val userIds = rdd.map(_.user_id).collect().mkString("'", "','", "'")

      val provinceSql = s"select * from gmall_province_info where id in (${provinceIds})"
      val userSql = s"select * from gmall_user_info where id in (${userIds})"


      val provinceInfoRDD = SparkSqlUtil
        .getRDD[ProvinceInfo](spark, provinceSql)
        .map(info => (info.id, info))
      val userInfoRDD = SparkSqlUtil.getRDD[UserInfo](spark, userSql)
        .map(info => (info.id, info))

      rdd.map(info=>(info.province_id.toString,info))
        .join(provinceInfoRDD)
        .map{
          case (province_id,(orderInfo,provinceInfo))=>
            orderInfo.province_name = provinceInfo.name
            orderInfo.province_area_code = provinceInfo.area_code
            orderInfo.province_iso_code = provinceInfo.iso_code
            orderInfo
        }.map(info=>(info.user_id.toString,info))
        .join(userInfoRDD)
        .map{
          case (user_id,(orderInfo,userInfo))=>{
            orderInfo.user_age_group = userInfo.age_group
            orderInfo.user_age_group = userInfo.gender_name
            orderInfo
          }
        }
    })


    resultDS.print(10)
    resultDS.foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.cache()
      rdd.filter(_.is_first_order)
        .map(order=>UserStatus(order.user_id,true))
        .saveToPhoenix("USER_STATUS",Seq("USER_ID","IS_CONSUMED"),zkUrl=Option("node:2181"))

      rdd.foreachPartition(orders=>{
        EsUtils.insertBulk(s"gmall_order_info${LocalDate.now()}",orders.map(info=>(info.id.toString,info)))
      })

      rdd.foreachPartition(orders=>{
        val producer = MyKafkaUtil.getKafkaProducer()
        orders.foreach(order=>{
          producer.send(new ProducerRecord[String,String]("dwd_order_info",Serialization.write(order)))
        })
        producer.close()
      })


      OffsetManager.saveOffsets(offsetRanges,groupId,topic)


    })

  }
}
