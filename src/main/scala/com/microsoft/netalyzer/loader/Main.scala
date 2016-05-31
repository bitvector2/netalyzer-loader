package com.microsoft.netalyzer.loader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    Utils.initDb(settings.cookedData, sqlContext)

    val newDf = Utils.dfZipWithIndex(
      Utils.loadCsvData(settings.rawData, sqlContext),
      Utils.getLastId(sqlContext) + 1
    )

    val paddedDf = newDf
      .withColumn("deltaseconds", lit(null: Integer))
      .withColumn("deltarxbytes", lit(null: Integer))
      .withColumn("deltatxbytes", lit(null: Integer))
      .withColumn("rxrate", lit(null: Integer))
      .withColumn("txrate", lit(null: Integer))
      .withColumn("rxutil", lit(null: Integer))
      .withColumn("txutil", lit(null: Integer))

    paddedDf.printSchema()
    paddedDf.write.mode("append").saveAsTable("netalyzer.samples")

  }
}
