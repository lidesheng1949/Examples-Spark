package com.ms

import org.apache.spark.sql.SparkSession


object BatchInvoke {

  def main(args: Array[String]) : Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    dataFrameExample(sparkSession,"default.sample_07")

    sparkSession.close()

  }

   def dataFrameExample(sparks: SparkSession,tableName:String): Unit ={
     val query= "select * from ".concat(tableName)
     val df = sparks.sql(query)

     df.printSchema()
    println( df.count())

  }


}
