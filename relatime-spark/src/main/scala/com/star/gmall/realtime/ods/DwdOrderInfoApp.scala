package com.star.gmall.realtime.ods
import java.time.LocalDate

import com.star.gmall.realtime.bean.{OrderInfo, UserStatus}
import com.star.gmall.realtime.util.{EsUtils, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object DwdOrderInfoApp extends BaseApp {
  override var appName: String = "DwdOrderInfoApp"
  override var groupId: String = "DwdOrderInfoApp"
  override var topic: String = "ods_order_info"

  implicit val f = org.json4s.DefaultFormats
  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {

    val firstOrderInfoDS = sourceStream.map(record => {
      JsonMethods.parse(record.value()).extract[OrderInfo]
    }).mapPartitions(orders => {
      val orderInfoList = orders.toList
      val userIds = orderInfoList.map(_.user_id).mkString(",")
      //g根据该批次的user_id查询是否是首单
      val userIdToConsumed = PhoenixUtil.query(s"select user_id,is_consumed from user_status where user_id in ($userIds)",
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
