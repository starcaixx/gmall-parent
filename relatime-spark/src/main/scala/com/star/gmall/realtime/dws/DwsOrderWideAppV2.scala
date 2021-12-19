package com.star.gmall.realtime.dws

import java.util.Properties

import com.star.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.star.gmall.realtime.ods.BaseAppV3
import com.star.gmall.realtime.util.{OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DwsOrderWideAppV2 extends BaseAppV3{
  implicit val format = f
  override var appName: String = "DwsOrderWideAppV2"
  override var groupId: String = "DwsOrderWideAppV2"
  override var topics: Seq[String] = Seq("dwd_order_info","dwd_order_detail")

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], topicToStreamMap: Map[String, DStream[ConsumerRecord[String, String]]]): Unit = {

    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    val orderInfoDS = topicToStreamMap("dwd_order_info").map(record => {
      val orderInfo = JsonMethods.parse(record.value()).extract[OrderInfo]
      (orderInfo.id, orderInfo)
    })

    val orderDetailDS = topicToStreamMap("dwd_order_detail").map(record => {
      val orderDetail = JsonMethods.parse(record.value()).extract[OrderDetail]
      (orderDetail.order_id, orderDetail)
    })

    val orderWideDS = orderInfoDS.fullOuterJoin(orderDetailDS)
      .mapPartitions(it => {
        val client = RedisUtil.getClient
        val result = it.flatMap {
          //order_info和order_detail的数据同时到达
          case (orderId, (Some(orderInfo), opt)) =>
            println(s"${orderId} some opt")
            //1，把order_info信息写入到缓存
            cacheOrderInfo(client, orderInfo)
            //2，把order_info的信息和order_detial的信息封装到一起

//            val orderWide = OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            //3，去order_detail的缓存中查找对应的信息，注意：需要删除ordre_detail中的信息
            //3。1 先获取和order_id相关的所有的key
            //3。2 根据key获取对应的value（order_detial）
            import scala.collection.JavaConverters._
            implicit val format = f
            val key = s"order_detail:${orderInfo.id}"
            val results = client.hgetAll(key).asScala.map {
              case (orderDetailId, orderDetailStr) =>
                val orderDetail = JsonMethods.parse(orderDetailStr).extract[OrderDetail]
                new OrderWide(orderInfo, orderDetail)
            }.toList

            client.del(key)
            if (opt.isDefined) {
              new OrderWide(orderInfo,opt.get)::results
            }else {
              results
            }
            //orderDetail到了，orderInfo没到
          case (orderId, (None, Some(orderDetail))) =>
            println(s"${orderId} none some")
            val orderInfoJson = client.get(s"order_info:${orderDetail.order_id}")
            if (orderInfoJson != null) {
              implicit val format = f
              val orderInfo = JsonMethods.parse(orderInfoJson).extract[OrderInfo]
              OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
            } else {
              cacheOrderDetail(client, orderDetail)
              Nil
            }
        }
        client.close()
        result
      })

    //cal amt
    val resultDS = orderWideDS.mapPartitions(orderWides => {
      val client = RedisUtil.getClient
      val result = orderWides.map(orderWide => {
        val preTotalKey = s"pre_total:${orderWide.order_id}"
        val preSharesKey = s"pre_shares:${orderWide.order_id}"

        val preTotalTmp = client.get(preTotalKey)
        val preTotal = if (preTotalTmp == null) 0d else preTotalTmp.toDouble

        val preSharesTmp = client.get(preSharesKey)
        val preShares = if (preSharesTmp == null) 0d else preSharesTmp.toDouble

        val current = orderWide.sku_price * orderWide.sku_num
        if (current == orderWide.original_total_amount - preTotal) {
          orderWide.final_detail_amount = orderWide.final_total_amount - preShares
          client.del(preTotalKey)
          client.del(preSharesKey)
          println("last detail")
        } else {
          val share = orderWide.sku_num * orderWide.sku_price * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount = Math.round(share * 100) / 100
          val d1 = client.incrByFloat(preTotalKey, current)
          val d2 = client.incrByFloat(preSharesKey, Math.round(share * 100) / 100)
          println(s"not last detail: ${d1},${d2}")
        }
        orderWide
      })
      client.close()
      result
    })

    //write clickhouse
    resultDS.foreachRDD(rdd=>{
      rdd.cache()
      println("timestamp... start")
      rdd.collect().foreach(println)
      val df = rdd.toDS()
      df.write
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置没有事务
        .option("numPartitions", "2") // 设置并发
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .mode("append")
        .jdbc("jdbc:clickhouse://node:8123/gmall", "order_wide", new Properties())
      println("timestamp... end")
      OffsetManager.saveOffsets(offsetRanges,groupId,topics)
    })
  }

  def cacheOrderInfo(client:Jedis,orderInfo:OrderInfo)={
//    implicit val f = org.json4s.DefaultFormats
    client.setex(s"order_info:${orderInfo.id}",60*10,Serialization.write(OrderInfo))
  }

  def cacheOrderDetail(client:Jedis,orderDetail: OrderDetail)={
//    implicit val f = org.json4s.DefaultFormats
    client.setex(s"order_detail:${orderDetail.order_id}:${orderDetail.id}",60*10,Serialization.write(orderDetail))
  }
}
