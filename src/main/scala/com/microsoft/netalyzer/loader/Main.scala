package com.microsoft.netalyzer.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    Utils.initDb(settings.cookedData, sqlContext)

    val newDf = Utils.loadCsvData(settings.rawData, sqlContext)
      .withColumn("deltaseconds", lit(null: Integer).cast(IntegerType))
      .withColumn("deltarxbytes", lit(null: Integer).cast(IntegerType))
      .withColumn("deltatxbytes", lit(null: Integer).cast(IntegerType))
      .withColumn("rxrate", lit(null: Integer).cast(IntegerType))
      .withColumn("txrate", lit(null: Integer).cast(IntegerType))
      .withColumn("rxutil", lit(null: Integer).cast(IntegerType))
      .withColumn("txutil", lit(null: Integer).cast(IntegerType))

    newDf.printSchema()
    newDf.write.mode("append").saveAsTable("netalyzer.samples")

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(settings.rawData), true)
  }
}
