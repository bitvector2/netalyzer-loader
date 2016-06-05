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
    println("checkpoint 1")

    val newDf = Utils.loadCsvData(settings.rawData, sqlContext)
    println("checkpoint 2")

    newDf.write.mode("overwrite").saveAsTable("netalyzer.samples")
    println("checkpoint 3")

    val deltasDf = sqlContext.sql(
      """
        INSERT OVERWRITE TABLE netalyzer.deltas
        SELECT datetime,
        hostname,
        portname,
          unix_timestamp(datetime) - lag(unix_timestamp(datetime)) OVER (PARTITION BY hostname, portname ORDER BY datetime) AS deltaseconds,
          CASE WHEN (lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) > totalrxbytes)
            THEN round(18446744073709551615 - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) + totalrxbytes)
            ELSE round(totalrxbytes - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime))
          END AS deltarxbytes,
          CASE WHEN (lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) > totaltxbytes)
            THEN round(18446744073709551615 - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) + totaltxbytes)
            ELSE round(totaltxbytes - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime))
          END AS deltatxbytes
        FROM netalyzer.samples
        ORDER BY hostname,
          portname,
          datetime
      """.stripMargin
    )
    println("checkpoint 4")

    deltasDf.printSchema()
    deltasDf.show()

    // FileSystem.get(sc.hadoopConfiguration).delete(new Path(settings.rawData), true)

  }
}
