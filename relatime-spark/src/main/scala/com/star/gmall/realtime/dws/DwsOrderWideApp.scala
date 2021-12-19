package com.star.gmall.realtime.dws

import com.star.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.star.gmall.realtime.ods.BaseAppV3
import com.star.gmall.realtime.util.{OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

object DwsOrderWideApp extends BaseAppV3{
  implicit val format = f
  override var appName: String = "DwsOrderWideApp"
  override var groupId: String = "DwsOrderWideApp"
  override var topics: Seq[String] = Seq("dwd_order_info","dwd_order_detail")

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], topicToStreamMap: Map[String, DStream[ConsumerRecord[String, String]]]): Unit = {
    //得到两个事实表的流，添加重叠的滑动窗口
    val orderInfoDS = topicToStreamMap("dwd_order_info").map(record => {
      val orderInfo = JsonMethods.parse(record.value()).extract[OrderInfo]
      (orderInfo.id, orderInfo)
    }).window(Seconds(60), Seconds(10))

    val orderDetailDS = topicToStreamMap("dwd_order_detail").map(record => {
      val orderDetail = JsonMethods.parse(record.value()).extract[OrderDetail]
      (orderDetail.order_id, orderDetail)
    }).window(Seconds(60), Seconds(10))

    //对两个流进行join，此处水涌内连接
    val orderWideDS = orderInfoDS.join(orderDetailDS).map {
      case (orderId, (orderInfo, orderDetail)) => new OrderWide(orderInfo, orderDetail)
    }

    //对重复join的数据进行去重
    val orderWideDistinctDS = orderWideDS.mapPartitions(orderWides => {
      val client = RedisUtil.getClient
      val result = orderWides.filter(orderWide => {
        //用两张表的主键作为key
        //key                 value
        //order_join:order_id order_detail_id
        val hasOrderId = client.sadd(s"order_join:${orderWide.order_id}",orderWide.order_detail_id.toString)
        client.expire(s"order_join:${orderWide.order_id}", 60 * 3)
        hasOrderId == 1
      })
      client.close()
      result
    })

    orderWideDistinctDS.foreachRDD(rdd=>{
      rdd.cache()
      println("timestamp... start")
      rdd.collect().foreach(println)
      println("timestamp... end")

      OffsetManager.saveOffsets(offsetRanges,groupId,topics)
    })

  }
}
