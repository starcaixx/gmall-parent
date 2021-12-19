package com.star.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      ar: String,
                      ba: String,
                      ch: String,
                      md: String,
                      os: String,
                      vc: String,
                      ts: Long,
                      var logDate: String = null, // 年月日  2020-07-15
                      var logHour: String = null) { //小时  10
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}

case class OrderInfo(id: Long,
                     province_id: Long,
                     order_status: String,
                     user_id: String,
                     total_amount: Double,
                     activity_reduce_amount: Double,
                     original_total_amount: Double,
                     feight_fee: Double,
                     expire_time: String,
                     create_time: String,
                     operate_time: String,
                     var create_date: String = null,
                     var create_hour: String = null,
                     var is_first_order: Boolean = false,

                     var province_name: String = null,
                     var province_area_code: String = null,
                     var province_iso_code:String = null, //国际地区编码

                     var user_age_group: String = null,
                     var user_gender: String = null) {
  create_date = create_time.substring(0, 10)
  create_hour = create_time.substring(11, 13)
}

case class UserStatus(user_id:String,is_consumed:Boolean)

case class ProvinceInfo(id: String,
                        name: String,
                        area_code: String,
                        iso_code: String)

/**
 * Author lzc
 * Date 2020/8/28 3:06 上午
 */
case class UserInfo(id: String,
                    user_level: String,
                    birthday: String,
                    gender: String, // F  M
                    var age_group: String = null, //年龄段
                    var gender_name: String = null) { //性别  男  女
  // 计算年龄段
  val age = (System.currentTimeMillis() - new SimpleDateFormat("yyyy-MM-dd").parse(birthday).getTime) / 1000 / 60 / 60 / 24 / 365
  age_group = if (age <= 20) "20岁及以下" else if (age <= 30) "21岁到 30 岁" else "30岁及以上"
  // 计算gender_name
  gender_name = if (gender == "F") "女" else "男"
}


case class BaseTrademark(id:String , tm_name:String)

case class BaseCategory3(id: String,
                         name: String,
                         category2_id: String)
case class SpuInfo(id: String, spu_name: String)

case class SkuInfo(id: String,
                   spu_id: String,
                   price: String,
                   sku_name: String,
                   tm_id: String,
                   category3_id: String,
                   create_time: String,

                   var category3_name: String = null,
                   var spu_name: String = null,
                   var tm_name: String = null)


case class OrderDetail(id: Long,
                       order_id: Long,
                       sku_id: Long,
                       order_price: Double,
                       sku_num: Long,
                       sku_name: String,
                       create_time: String,

                       var spu_id: Long = 0L, //作为维度数据 要关联进来
                       var tm_id: Long = 0L,
                       var category3_id: Long = 0L,
                       var spu_name: String = null,
                       var tm_name: String = null,
                       var category3_name: String = null) {
  def mergeSkuInfo(skuInfo: SkuInfo) = {
    this.spu_id = skuInfo.spu_id.toLong
    this.tm_id = skuInfo.tm_id.toLong
    this.category3_id = skuInfo.category3_id.toLong
    this.spu_name = skuInfo.spu_name
    this.tm_name = skuInfo.tm_name
    this.category3_name = skuInfo.category3_name
    this
  }
}


case class OrderWide( // 来源 OrderInfo
                      var order_id: Long = 0L,
                      var province_id: Long = 0L,
                      var order_status: String = null,
                      var user_id: String = null,
                      var final_total_amount: Double = 0D, // 实际支付总金额 = 原始总金额-优惠+运费
                      var benefit_reduce_amount: Double = 0D, // 优惠金额
                      var original_total_amount: Double = 0D, // 原始总金额 = ∑sku_price*sku_num
                      var feight_fee: Double = 0D, // 运费
                      var expire_time: String = null,
                      var create_time: String = null,
                      var operate_time: String = null,
                      var create_date: String = null,
                      var create_hour: String = null,
                      var is_first_order: Boolean = false,

                      var province_name: String = null,
                      var province_area_code: String = null,
                      var province_iso_code: String = null,

                      var user_age_group: String = null,
                      var user_gender: String = null,

                      //来源: OderDetail
                      var order_detail_id: Long = 0L,
                      var sku_id: Long = 0L,
                      var sku_price: Double = 0L, // 在 OrderDetail 中叫 order_price
                      var sku_num: Long = 0L,
                      var sku_name: String = null,

                      var spu_id: Long = 0L,
                      var tm_id: Long = 0L,
                      var category3_id: Long = 0L,
                      var spu_name: String = null,
                      var tm_name: String = null,
                      var category3_name: String = null,

                      // 需要计算的分摊金额
                      var final_detail_amount: Double = 0D) {
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  def mergeOrderInfo(orderInfo: OrderInfo): OrderWide = {
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.province_id = orderInfo.province_id
      this.order_status = orderInfo.order_status
      this.user_id = orderInfo.user_id
      this.final_total_amount = orderInfo.total_amount
      this.benefit_reduce_amount = orderInfo.activity_reduce_amount
      this.original_total_amount = orderInfo.original_total_amount
      this.feight_fee = orderInfo.feight_fee
      this.expire_time = orderInfo.expire_time
      this.create_time = orderInfo.create_time
      this.create_date = orderInfo.create_date
      this.create_hour = orderInfo.create_hour
      this.is_first_order = orderInfo.is_first_order
      this.province_name = orderInfo.province_name
      this.province_area_code = orderInfo.province_area_code
      this.province_iso_code = orderInfo.province_iso_code
      this.user_age_group = orderInfo.user_age_group
      this.user_gender = orderInfo.user_gender
    }
    this
  }
  def mergeOrderDetail(orderDetail: OrderDetail): OrderWide = {
    if (orderDetail != null) {
      this.order_detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_num = orderDetail.sku_num
      this.sku_name = orderDetail.sku_name
      this.sku_price = orderDetail.order_price

      this.spu_id = orderDetail.spu_id
      this.tm_id = orderDetail.tm_id
      this.category3_id = orderDetail.category3_id
      this.spu_name = orderDetail.spu_name
      this.tm_name = orderDetail.tm_name
      this.category3_name = orderDetail.category3_name
    }
    this
  }
}


