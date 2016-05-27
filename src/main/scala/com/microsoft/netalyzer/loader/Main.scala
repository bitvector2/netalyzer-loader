package com.microsoft.netalyzer.loader


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
//    val newDf = Utils.dfZipWithIndex(
//      Utils.loadCsvData(settings.rawData, sqlContext),
//      Utils.getLastId(settings.preppedData, sqlContext) + 1  // Runtime with grow with time
//    )
//    Utils.appendOrcData(settings.preppedData, newDf)
//    newDf.unpersist()
//    Utils.deleteCsvData(settings.rawData, sc)


    sqlContext.sql(
      """
        CREATE DATABASE IF NOT EXISTS netalyzer
        LOCATION "/Microsoft/NetalyzerJobs/CookedData"
      """.stripMargin
    )

    sqlContext.sql(
      """
        CREATE TABLE IF NOT EXISTS netalyzer.samples (
          id BIGINT,
          datetime TIMESTAMP,
          hostname VARCHAR(255),
          portname VARCHAR(255),
          portspeed BIGINT,
          totalrxbytes BIGINT,
          totaltxbytes BIGINT,
          deltaseconds INT,
          deltarxbytes INT,
          deltatxbytes INT,
          rxrate INT,
          txrate INT,
          rxutil INT,
          txutil INT
        )
        CLUSTERED BY(id) INTO 200 BUCKETS
        STORED AS ORC
        TBLPROPERTIES("transactional"="true")
      """.stripMargin
    )

    val data = sqlContext.sql(
      """
         SELECT 1 AS id,
         null AS datetime,
         null AS hostname,
         null AS portname,
         null AS portspeed,
         null AS totalrxbytes,
         null AS totaltxbytes,
         null AS deltaseconds,
         null AS deltarxbytes,
         null AS deltatxbytes,
         null AS txrate,
         null AS rxrate,
         null AS rxutil,
         null AS txutil
      """.stripMargin
    )

    data.write.mode("append").saveAsTable("netalyzer.samples")
  }
}
