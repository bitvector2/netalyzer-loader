package com.microsoft.netalyzer.loader

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

object Utils {

  def initDb(path: String, sc: SQLContext) = {
    sc.sql(
      s"""
        CREATE DATABASE IF NOT EXISTS netalyzer
        LOCATION "$path"
      """.stripMargin
    )

    sc.sql(
      """
        CREATE TABLE IF NOT EXISTS netalyzer.samples (
          datetime TIMESTAMP,
          hostname VARCHAR(255),
          portname VARCHAR(255),
          portspeed DECIMAL(38,0),
          totalrxbytes DECIMAL(38,0),
          totaltxbytes DECIMAL(38,0),
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
  }

  // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
  def loadCsvData(path: String, sc: SQLContext): DataFrame = {
    sc.setConf("spark.sql.shuffle.partitions", "200")

    val customSchema = StructType(
      Array(
        StructField("datetime", TimestampType, nullable = false),
        StructField("hostname", StringType, nullable = false),
        StructField("portname", StringType, nullable = false),
        StructField("portspeed", DecimalType(38, 0), nullable = false),
        StructField("totalrxbytes", DecimalType(38, 0), nullable = false),
        StructField("totaltxbytes", DecimalType(38, 0), nullable = false)
      )
    )

    var newDf = sc.emptyDataFrame

    try {
      newDf = sc.read
        .format("com.databricks.spark.csv")
        .option("mode", "FAILFAST")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(customSchema)
        .load(path)
        .repartition(200)
    }
    catch {
      case e: InvalidInputException =>
        println("loadCsvData() caught an exception: " + e.getMessage)
        e.printStackTrace()
      case e: RuntimeException =>
        println("loadCsvData() caught an exception: " + e.getMessage)
        e.printStackTrace()
    }

    newDf
  }

  // http://www.cisco.com/c/en/us/support/docs/ip/simple-network-management-protocol-snmp/26007-faq-snmpcounter.html
  def add1stDeltas(df: DataFrame, sc: SQLContext): DataFrame = {
    df.registerTempTable("df")
    val newdf = sc.sql(
      """
      SELECT id,
        unix_timestamp(datetime) - lag(unix_timestamp(datetime)) OVER (PARTITION BY hostname, portname ORDER BY datetime) AS deltaseconds,
        CASE WHEN (lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) > totalrxbytes)
          THEN round(18446744073709551615 - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) + totalrxbytes)
          ELSE round(totalrxbytes - lag(totalrxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime))
        END AS deltarxbytes,
        CASE WHEN (lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) > totaltxbytes)
          THEN round(18446744073709551615 - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime) + totaltxbytes)
          ELSE round(totaltxbytes - lag(totaltxbytes) OVER (PARTITION BY hostname, portname ORDER BY datetime))
        END AS deltatxbytes
      FROM df
      ORDER BY hostname,
        portname,
        datetime
      """
    )
    sc.dropTempTable("df")
    newdf
  }

  def add1stDerivs(df: DataFrame, sc: SQLContext): DataFrame = {
    df.registerTempTable("df")
    val newdf = sc.sql(
      """
      SELECT id,
        deltaseconds,
        deltarxbytes,
        deltatxbytes,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltarxbytes / deltaseconds) END AS rxrate,
        CASE WHEN (deltaseconds = 0) THEN null ELSE round(deltatxbytes / deltaseconds) END AS txrate
      FROM df
      """
    )
    sc.dropTempTable("df")
    newdf
  }

  def addUtilzs(df: DataFrame, sc: SQLContext): DataFrame = {
    df.registerTempTable("df")
    val newdf = sc.sql(
      """
      SELECT id,
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
    sc.dropTempTable("df")
    newdf
  }

}
