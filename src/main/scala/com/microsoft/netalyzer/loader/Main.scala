package com.microsoft.netalyzer.loader


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    val lastId = Utils.getLastId(settings.preppedData, sqlContext)
    val rawDf = Utils.loadCsvData(settings.rawData, sqlContext)
    val newDf = Utils.dfZipWithIndex(rawDf, lastId + 1)

    Utils.appendOrcData(settings.preppedData, newDf)
    Utils.deleteCsvData(settings.rawData, sc)
  }
}
