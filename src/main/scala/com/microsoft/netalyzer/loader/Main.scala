package com.microsoft.netalyzer.loader

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Main {

  val settings = new Settings()
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  def main(args: Array[String]) = {
    sqlContext.setConf("spark.sql.shuffle.partitions", "200")

    val customSchema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("hostname", StringType, nullable = false),
        StructField("portname", StringType, nullable = false),
        StructField("portspeed", DecimalType(38, 0), nullable = false),
        StructField("totalrxbytes", DecimalType(38, 0), nullable = false),
        StructField("totaltxbytes", DecimalType(38, 0), nullable = false)
      )
    )

    try {
      // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
      val rawDf = sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("mode", "FAILFAST")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(customSchema)
        .load(settings.inputDataSpec)
        .repartition(200)

      val cookedDf = sqlContext
        .read
        .format("orc")
        .load(settings.inputDataSpec)

      val nextId = cookedDf.select(max(cookedDf("id"))+1).first().getInt(1)

      val newDf = dfZipWithIndex(rawDf, nextId)

      println("New Cooked Data:  " + newDf.count() + " (rows) ")
      newDf.printSchema()
      newDf.show(1000)

      newDf.write
        .format("orc")
        .mode("append")
        .save(settings.outputDataSpec)

    } catch {
      case e: RuntimeException => handleRE(e)
    }
  }

  // http://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
  def dfZipWithIndex(df: DataFrame, offset: Int = 1, colName: String = "id", inFront: Boolean = true): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, nullable = false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, nullable = false)))
      )
    )
  }

  def handleRE(e: RuntimeException) = {
    println("Caught Runtime Exceptions: " + e.getMessage)
    sys.exit(69)
  }

  // http://www.cisco.com/c/en/us/support/docs/ip/simple-network-management-protocol-snmp/26007-faq-snmpcounter.html
  def add1stDeltas(df: DataFrame): DataFrame = {
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

  def add1stDerivs(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        portspeed,
        totalrxbytes,
        totaltxbytes,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltarxbytes / deltaseconds) END AS rxrate,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltatxbytes / deltaseconds) END AS txrate
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
        portspeed,
        totalrxbytes,
        totaltxbytes,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        rxrate,
        txrate,
        CASE WHEN (portspeed = 0) THEN null ELSE round(rxrate / portspeed * 800) END AS rxutil,
        CASE WHEN (portspeed = 0) THEN null ELSE round(txrate / portspeed * 800) END AS txutil
      FROM df
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

  def add2ndDeltas(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        portspeed,
        totalrxbytes,
        totaltxbytes,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        rxrate,
        txrate,
        rxutil,
        txutil,
        round(rxrate - lag(rxrate) OVER (PARTITION BY hostname, portname ORDER BY timestamp)) AS deltarxrate,
        round(txrate - lag(txrate) OVER (PARTITION BY hostname, portname ORDER BY timestamp)) AS deltatxrate
      FROM df
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

  def add2ndDerivs(df: DataFrame): DataFrame = {
    df.registerTempTable("df")
    val newdf = sqlContext.sql(
      """
      SELECT timestamp,
        hostname,
        portname,
        portspeed,
        totalrxbytes,
        totaltxbytes,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        rxrate,
        txrate,
        rxutil,
        txutil,
        deltarxrate,
        deltatxrate,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltarxrate / deltaseconds) END AS rxaccel,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltatxrate / deltaseconds) END AS txaccel
      FROM df
      """
    )
    sqlContext.dropTempTable("df")
    newdf
  }

}
