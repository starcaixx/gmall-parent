package com.star.gmall.realtime.util

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

object EsUtils {
  val esUrl = "http://node:9200"

  private val factory = new JestClientFactory
  private val conf: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .connTimeout(1000 * 10)
    .readTimeout(1000 * 10)
    .maxTotalConnection(100)
    .multiThreaded(true)
    .build()

  factory.setHttpClientConfig(conf)

  //批量插入
  def insertBulk (index:String,source:Iterator[Object])={
    val client = factory.getObject

    val builder = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")

    source.foreach{
      case(id:String,source)=>
        val index = new Index.Builder(source).id(id)
        builder.addAction(index.build())
      case source =>
        val index = new Index.Builder(source)
        builder.addAction(index.build())
    }
    client.execute(builder.build())
    client.shutdownClient()
  }

  def insertSingle(index:String,source:Object,id:String=null) ={
    val client = factory.getObject
    val action = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()

    client.execute(action)
    client.shutdownClient()
  }


}
