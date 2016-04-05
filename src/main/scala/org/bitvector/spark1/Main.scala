package org.bitvector.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  val settings = new Settings()
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    logger.info("Starting with:  " + settings.foo)

    val conf = new SparkConf().setAppName("SparkSQLTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // https://docs.oracle.com/javase/7/docs/api/java/sql/Timestamp.html
    val customSchema = StructType(Array(
      StructField("Timestamp", TimestampType, nullable = false),
      StructField("Hostname", StringType, nullable = false),
      StructField("PortName", StringType, nullable = false),
      StructField("PortBitsPerSecond", LongType, nullable = false),
      StructField("RxTotalBytes", LongType, nullable = false),
      StructField("TxTotalBytes", LongType, nullable = false)
    ))

    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    // https://github.com/databricks/spark-csv
    val rawDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSX")
      .schema(customSchema)
      .load("wasb:///tmp/test_data.csv")

    val cookedDf = rawDf
      .transform(addThroughput)
      .transform(addUtilization)

    cookedDf.show()

    logger.info("Stopping...")
  }

  def addThroughput(df: DataFrame): DataFrame = {
    val windowsSpec = Window.partitionBy("Hostname", "PortName").orderBy("Timestamp").rowsBetween(-1, 0)
    df
  }

  def addUtilization(df: DataFrame): DataFrame = {
    val windowsSpec = Window.partitionBy("Hostname", "PortName").orderBy("Timestamp").rowsBetween(-1, 0)
    df
  }
}
