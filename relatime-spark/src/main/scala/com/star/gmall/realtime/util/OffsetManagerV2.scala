package com.star.gmall.realtime.util

import org.apache.kafka.common.TopicPartition

object OffsetManagerV2 {
  def readOffset(groupId:String, topic: String) = {
    val url = "jdbc:mysql://hadoop102:3306/gmall_result?characterEncoding=utf-8&useSSL=false&user=root&password=aaaaaa"
    val sql =
      """
        |select
        | *
        |from ads_offset
        |where topic=? and group_id=?
        |""".stripMargin
    JDBCUtil
      .query(url, sql, List(topic, groupId))
      .map(row => {
        val partitionId = row("partition_id").toString.toInt
        val partitionOffset = row("partition_offset").toString.toLong
        (new TopicPartition(topic, partitionId), partitionOffset)
      })
      .toMap
  }

}
