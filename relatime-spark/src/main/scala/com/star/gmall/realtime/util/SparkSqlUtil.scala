package com.star.gmall.realtime.util

import org.apache.spark.sql.{Encoder, SparkSession}

object SparkSqlUtil {
  val url = "jdbc:phoenix:node:2181"

  def getRDD[C:Encoder](spark:SparkSession,sql:String)={
    spark.read
      .format("jdbc")
      .option("url",url)
      .option("query",sql)
      .load()
      .as[C]
      .rdd
  }
}
