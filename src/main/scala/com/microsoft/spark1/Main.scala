package com.microsoft.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val settings = new Settings()
  val logger = Logger.getLogger(getClass.getName)
  val conf = new SparkConf().setAppName("SparkSQLTest")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

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
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .schema(customSchema)
      .load(settings.inputDataSpec)
      .orderBy("Hostname", "PortName", "Timestamp")

    val cookedDf = dfZipWithIndex(rawDf, colName = "Id", inFront = true)
      .transform(deltaTime)
      .transform(deltaRxBytes)
      .transform(deltaTxBytes)
      .persist()

    println("Cooked Data:")
    cookedDf.printSchema()
    cookedDf.show()

    cookedDf.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(settings.outputDataSpec)

    logger.info("Finished with:  " + settings.outputDataSpec)
  }

  // http://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex - WOW
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

  def deltaTime(df: DataFrame): DataFrame = {
    // FIXME
    df
  }

  def deltaRxBytes(df: DataFrame): DataFrame = {
    // FIXME
    df
  }

  def deltaTxBytes(df: DataFrame): DataFrame = {
    // FIXME
    df
  }
}
