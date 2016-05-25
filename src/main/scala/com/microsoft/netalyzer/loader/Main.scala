package com.microsoft.netalyzer.loader


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    val newDf = Utils.dfZipWithIndex(
      Utils.loadCsvData(settings.rawData, sqlContext),
      Utils.getLastId(settings.preppedData, sqlContext) + 1  // Runtime with grow with time
    )
    Utils.appendOrcData(settings.preppedData, newDf)
    newDf.unpersist()
    Utils.deleteCsvData(settings.rawData, sc)



    val preppedDf = Utils.loadOrcData(settings.preppedData, sqlContext)
    val deltasDf = Utils.add1stDeltas(preppedDf, sqlContext)
    val derivsDf = Utils.add1stDerivs(deltasDf, sqlContext)
    // val utilzsDf = Utils.addUtilzs(derivsDf,sqlContext)


    derivsDf.printSchema()
    derivsDf.show()



  }
}
