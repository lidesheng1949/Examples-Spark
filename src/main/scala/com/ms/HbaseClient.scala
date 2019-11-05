package com.ms


import org.apache.hadoop.hbase.client.{ Put, Get}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes


object HbaseClient {

  def main(args: Array[String]): Unit = {

    val hbaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf("t")
    hbaseConf.set("hbase.zookeeper.quorum","mstestspark-2.vpc.cloudera.com,mstestspark-3.vpc.cloudera.com,mstestspark-1.vpc.cloudera.com")
    hbaseConf.set("hbase.zookeeper.property.client.port","2181")
    val  conn =ConnectionFactory.createConnection(hbaseConf)
    val admin=conn.getAdmin
    val cf = Bytes.toBytes("f")
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(cf))
    }
    val hTable =conn.getTable(tableName)
    val put = new Put(Bytes.toBytes("8"))
    val cq = Bytes.toBytes("Value")
    val value = Bytes.toBytes("1")
    put.addColumn(cf,cq,value)
   // hTable.put(put)
    val getResult=hTable.get(new Get(Bytes.toBytes("90")))
    println(getResult)
    val greeting = Bytes.toString(getResult.getValue(cf, cq))
    val key=Bytes.toString(getResult.getRow)
    if(key == null){
      println("key")
    }

    println(key)
    hTable.close()

  }
}
