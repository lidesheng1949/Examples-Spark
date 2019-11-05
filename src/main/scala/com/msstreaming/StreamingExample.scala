package com.msstreaming

import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import java.time.LocalDateTime


object StreamingExample {

  def checkData(p: ConsumerRecord[String, String], hbaseTable: Table): Boolean = {

    var filter=true
    val recordValue = p.value()
    val getResult=hbaseTable.get(new Get(Bytes.toBytes(recordValue.split(",")(0))))
    val hkey= Bytes.toString(getResult.getRow)
    if(hkey == null)
      filter = false
    else
      filter = false

    filter
  }
//DAta Ex: kafka key->null kafka value->1[hbasekey),2[hbasevalue)
  def checkDataAndInsert(p: ConsumerRecord[String, String], hbaseTable: Table): Unit = {


    val recordValue = p.value().toString//1,2
    val cf = Bytes.toBytes("f") //conf
    val key=recordValue.split(",")(0)//1
    val getResult=hbaseTable.get(new Get(Bytes.toBytes(key))) //hbase get
    val hkey= Bytes.toString(getResult.getRow)
    if(hkey == null){
      val put = new Put(Bytes.toBytes(key))
      println("Inserting  Record.Key:"+key)
      val cq = Bytes.toBytes("Value")
      val value = Bytes.toBytes(recordValue.split(",")(1))
      put.addColumn(cf,cq,value)
      hbaseTable.put(put)
    }
    else
      {
        println("Record already present.Key:"+hkey)
      }
  }
  def main(args: Array[String]): Unit = {
    val kafkaPrms = Map[String, Object](
      "bootstrap.servers" -> "mstestspark-4.vpc.cloudera.com:9092,mstestspark-5.vpc.cloudera.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "t8",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = "hbasetest"
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(25))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet


    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaPrms))


      messages.foreachRDD(rdd =>
      rdd.foreachPartition(partitionOfRecords => {
        val hbaseConf = HBaseConfiguration.create()
        val tableName = TableName.valueOf("t")
        hbaseConf.set("hbase.zookeeper.quorum","mstestspark-2.vpc.cloudera.com,mstestspark-3.vpc.cloudera.com,mstestspark-1.vpc.cloudera.com")
        hbaseConf.set("hbase.zookeeper.property.client.port","2181")
        val  conn =ConnectionFactory.createConnection(hbaseConf)
        val hTable =conn.getTable(tableName)

        partitionOfRecords.foreach(r => checkDataAndInsert(r,hTable))

        hTable.close()
        conn.close()
      }))
/*    messages.foreachRDD(rdd =>
      {

        val time=LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
        rdd.map(x => x.value()).saveAsTextFile("/tmp/output/"+time)
      }
      )*/
/*       messages.foreachRDD(rdd =>
        {
          val hbaseConf = HBaseConfiguration.create()
          val tableName = TableName.valueOf("t")
          hbaseConf.set("hbase.zookeeper.quorum","mstestspark-2.vpc.cloudera.com,mstestspark-3.vpc.cloudera.com,mstestspark-1.vpc.cloudera.com")
          hbaseConf.set("hbase.zookeeper.property.client.port","2181")
          val  conn =ConnectionFactory.createConnection(hbaseConf)
          val hTable =conn.getTable(tableName)
          rdd.filter(x => checkData(x,hTable))
          val time=LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
          rdd.map(x => x.value()).saveAsTextFile("/tmp/output/"+time)
          hTable.close()
          conn.close()
         })*/
    ssc.start()
    ssc.awaitTermination()
  }
}

