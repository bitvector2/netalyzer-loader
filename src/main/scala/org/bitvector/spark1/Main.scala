package org.bitvector.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object Main {
  val settings = new Settings()
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    logger.info("Starting with:  " + settings.foo)

    val conf = new SparkConf().setAppName("SparkSQLTest")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // https://docs.oracle.com/javase/7/docs/api/java/sql/Timestamp.html
    val customSchema = StructType(Array(
      StructField("Timestamp", TimestampType, nullable = false),
      StructField("Hostname", StringType, nullable = false),
      StructField("PortName", StringType, nullable = false),
      StructField("PortSpeed", LongType, nullable = false),
      StructField("TotalRxBytes", LongType, nullable = false),
      StructField("TotalTxBytes", LongType, nullable = false)
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
      .transform(deltaTime)
      .transform(deltaRxBytes)
      .transform(deltaTxBytes)

    cookedDf.printSchema()
    cookedDf.show()

    logger.info("Stopping...")
  }

  def deltaTime(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Hostname", "PortName").orderBy("Timestamp")
    df.withColumn( "DeltaTime", df("Timestamp") - lag(df("Timestamp"), 1).over(windowSpec) )
  }

  def deltaRxBytes(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Hostname", "PortName").orderBy("Timestamp")
    df.withColumn( "DeltaRxBytes", df("TotalRxBytes") - lag(df("TotalRxBytes"), 1).over(windowSpec) )
  }

  def deltaTxBytes(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Hostname", "PortName").orderBy("Timestamp")
    df.withColumn( "DeltaTxBytes", df("TotalTxBytes") - lag(df("TotalTxBytes"), 1).over(windowSpec) )
  }
}
