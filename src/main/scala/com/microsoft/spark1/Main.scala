package com.microsoft.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val logger = Logger.getLogger(getClass.getName)
  val conf = new SparkConf().setAppName("SparkSQLTest")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) = {
    logger.info("Starting with:  " + settings.inputDataSpec)

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
      .load(settings.inputDataSpec)
      .orderBy("Hostname", "PortName", "Timestamp")

    val cookedDf = rawDf
      .transform(addIdColumn)

    println("Cooked Data:")
    cookedDf.printSchema()
    cookedDf.show()

    logger.info("Finished with:  " + settings.outputDataSpec)
  }

  def addIdColumn(df: DataFrame): DataFrame = {
    val numbers = sc.parallelize(Range.Long(0, df.count(), 1)).toDF("Id")
    val ids = numbers("Id")

    df.withColumn("Id", ids)
  }

}
