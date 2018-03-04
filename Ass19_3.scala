package com.spark.streaming

import org.apache.spark.sql.SparkSession

object Ass19_3 extends App {

  val sparkSession = SparkSession.builder.master("local").appName("spark").config("spark.sql.warehouse.dir","file:///C:/Users").getOrCreate()
  val values = 1 to 100 toList
  val newList = values.map(Tuple1(_))
  val df = sparkSession.createDataFrame(newList).toDF()
  df.show()
  df.write.format("parquet").save("databricks-df-example")
  val parquetDF = sparkSession.sqlContext.read.parquet("databricks-df-example")
  parquetDF.show()
}
