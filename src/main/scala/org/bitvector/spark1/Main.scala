package org.bitvector.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    logger.info("Starting with:  " + settings.foo)

    val conf = new SparkConf().setAppName("SparkSQLTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("wasb:///tmp/test_data.csv")

    df.printSchema()
    df.show()

    logger.info("Stopping...")
  }
}
