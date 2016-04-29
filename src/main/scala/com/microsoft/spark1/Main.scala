package com.microsoft.spark1

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val settings = new Settings()
  val conf = new SparkConf().setAppName("NetalyzerJob")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]) = {
    val customSchema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("hostname", StringType, nullable = false),
        StructField("portname", StringType, nullable = false),
        StructField("portspeed", LongType, nullable = false),
        StructField("totalrxbytes", DecimalType(38, 0), nullable = false),
        StructField("totaltxbytes", DecimalType(38, 0), nullable = false)
      )
    )

    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    val rawDf = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("mode", "FAILFAST")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .schema(customSchema)
      .load(settings.inputDataSpec)

    val cookedDf = rawDf
      .transform(addDeltas)
      .transform(addRates)
      .transform(addUtilzs)

    println("Cooked Data:  " + cookedDf.count() + " (rows) ")
    cookedDf.printSchema()
    cookedDf.show(1000)

    // http://hortonworks.com/blog/bringing-orc-support-into-apache-spark/
    cookedDf
      .write
      .format("orc")
      .mode("overwrite")
      .save(settings.outputDataSpec)
  }

  def addDeltas(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        portspeed,
        totalrxbytes,
        totaltxbytes,
        unix_timestamp(timestamp) - lag(unix_timestamp(timestamp)) OVER (PARTITION BY hostname, portname ORDER BY timestamp) AS deltaseconds,
        CASE WHEN (lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp) > totalrxbytes)
          THEN round(18446744073709551615 - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp) + totalrxbytes)
          ELSE round(totalrxbytes - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp))
        END AS deltarxbytes,
        CASE WHEN (lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp) > totaltxbytes)
          THEN round(18446744073709551615 - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp) + totaltxbytes)
          ELSE round(totaltxbytes - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY timestamp))
        END AS deltatxbytes
      FROM df
      ORDER BY hostname,
        portname,
        timestamp
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

  def addRates(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        portspeed,
        totalrxbytes,
        totaltxbytes,
        CASE WHEN (deltaseconds = 0) THEN null ELSE deltaseconds END AS deltaseconds,
        deltarxbytes,
        deltatxbytes,
        round(deltarxbytes / deltaseconds) AS rxrate,
        round(deltatxbytes / deltaseconds) AS txrate
      FROM df
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

  def addUtilzs(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        CASE WHEN (portspeed = 0) THEN null ELSE portspeed END AS portspeed,
        totalrxbytes,
        totaltxbytes,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        rxrate,
        txrate,
        round(rxrate / portspeed * 800) AS rxutil,
        round(txrate / portspeed * 800) AS txutil
      FROM df
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

}
