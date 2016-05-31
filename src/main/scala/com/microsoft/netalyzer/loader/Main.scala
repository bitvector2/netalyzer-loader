package com.microsoft.netalyzer.loader


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

    newDf.printSchema()
    newDf.show()



//    val data = sqlContext.sql(
//      """
//         SELECT 1 AS id,
//         null AS datetime,
//         null AS hostname,
//         null AS portname,
//         null AS portspeed,
//         null AS totalrxbytes,
//         null AS totaltxbytes,
//         null AS deltaseconds,
//         null AS deltarxbytes,
//         null AS deltatxbytes,
//         null AS txrate,
//         null AS rxrate,
//         null AS rxutil,
//         null AS txutil
//      """.stripMargin
//    )
//
//    data.write.mode("append").saveAsTable("netalyzer.samples")

  }
}
