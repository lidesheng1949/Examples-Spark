package com.structuredstreaming

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}

object StructuredStreamingExample {

  def checkData(p: ConsumerRecord[String, String], hbaseTable: Table): Boolean = {

    var filter = true
    val recordValue = p.value()
    val getResult = hbaseTable.get(new Get(Bytes.toBytes(recordValue.split(",")(0))))
    val hkey = Bytes.toString(getResult.getRow)
    if (hkey == null)
      filter = false
    else
      filter = false

    filter
  }

  def checkDataAndInsert(p: ConsumerRecord[String, String], hbaseTable: Table): Unit = {
    val recordValue = p.value().toString
    val cf = Bytes.toBytes("f")
    val key = recordValue.split(",")(0)
    val getResult = hbaseTable.get(new Get(Bytes.toBytes(key)))
    val hkey = Bytes.toString(getResult.getRow)
    if (hkey == null) {
      val put = new Put(Bytes.toBytes(key))
      println("Inserting  Record.Key:" + key)
      val cq = Bytes.toBytes("Value")
      val value = Bytes.toBytes(recordValue.split(",")(1))
      put.addColumn(cf, cq, value)
      hbaseTable.put(put)
    }
    else {
      println("Record already present.Key:" + hkey)
    }
  }

  def checkData(x: Row, hbaseTable: Table): Boolean = {
    val key = x.getAs[String]("key")
    val recordValue = x.getAs[String]("value")
    var filter = true
    val getResult = hbaseTable.get(new Get(Bytes.toBytes(recordValue.split(",")(0))))
    val hkey = Bytes.toString(getResult.getRow)
    if (hkey == null)
      filter = false
    else
      filter = false
    filter
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
    val intopic = "hbasetest"
    val outtopic = "hbasetestout"
    // Create context with 2 second batch interval
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "mstestspark-4.vpc.cloudera.com:9092,mstestspark-5.vpc.cloudera.com:9092")
      .option("subscribe", intopic)
      .option("startingOffsets", "earliest")
      .load()

    val hbaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf("t")
    hbaseConf.set("hbase.zookeeper.quorum", "mstestspark-2.vpc.cloudera.com,mstestspark-3.vpc.cloudera.com,mstestspark-1.vpc.cloudera.com")
    hbaseConf.set("hbase.zookeeper.property.client.port", "2181")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val hTable = conn.getTable(tableName)

    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").filter(x => checkData(x, hTable))



    ds.writeStream.format("kafka")
      .option("kafka.bootstrap.servers", "mstestspark-4.vpc.cloudera.com:9092,mstestspark-5.vpc.cloudera.com:9092")
      .option("topic", outtopic)
      .option("checkpointLocation", "/tmp/output")
      .start()

  }
}

