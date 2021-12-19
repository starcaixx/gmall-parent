package com.star.gmall.realtime.dwd

import com.star.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.star.gmall.realtime.ods.BaseApp
import com.star.gmall.realtime.util.{MyKafkaUtil, OffsetManager, SparkSqlUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object DwdOrderDetailApp extends BaseApp{
  override var appName: String = "DwdOrderDetailApp"
  override var groupId: String = "DwdOrderDetailApp"
  override var topic: String = "ods_order_detail"

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
    //1,data to bean
    val orderDetailDS = sourceStream.map(record => {
//      implicit val f = org.json4s.DefaultFormats + StringToLong
      implicit  val format = f + toLong
      JsonMethods.parse(record.value()).extract[OrderDetail]
    })

    val spark = SparkSession.builder().config(ssc.sparkContext.getConf)
      .getOrCreate()
    import spark.implicits._
    orderDetailDS.foreachRDD(rdd=>{
      rdd.cache()
      val skuIds = rdd.map(_.sku_id).collect().distinct
      if (skuIds.length>0) {
        val skuIdToOrderDetail = rdd.map(orderDetail => (orderDetail.sku_id.toString, orderDetail))

        val skuIdToSkuInfo = SparkSqlUtil.getRDD[SkuInfo](spark, s"select * from gmall_sku_info where id in ('${skuIds.mkString("','")}')")
          .map(skuInfo => (skuInfo.id, skuInfo))

        skuIdToOrderDetail.join(skuIdToSkuInfo).map{
          case(skuId,(detail,sku))=>
            detail.mergeSkuInfo(sku)
        }.foreachPartition(orderDetail=>{
          val producer = MyKafkaUtil.getKafkaProducer()
          orderDetail.foreach(order=>{
            implicit val f = org.json4s.DefaultFormats
            producer.send(new ProducerRecord[String,String]("dwd_order_detail",Serialization.write(order)))
          })
          producer.close()

        })
      }
      OffsetManager.saveOffsets(offsetRanges,groupId,topic)
    })



  }
}