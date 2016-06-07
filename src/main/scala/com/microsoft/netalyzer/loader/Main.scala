package com.microsoft.netalyzer.loader

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
    sqlContext.setConf("spark.sql.shuffle.partitions", "200")

    Utils.initDb(settings.cookedData, sqlContext)
    Utils.loadCsvData(settings.rawData, sqlContext)
    Utils.materializeDeltas(sqlContext)
  }
}
