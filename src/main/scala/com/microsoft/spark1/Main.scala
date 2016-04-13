package com.microsoft.spark1

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
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
    val customSchema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("hostname", StringType, nullable = false),
        StructField("portname", StringType, nullable = false),
        StructField("portspeed", LongType, nullable = false),
        StructField("totalrxbytes", LongType, nullable = false),
        StructField("totaltxbytes", LongType, nullable = false)
      )
    )

    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    val rawDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .schema(customSchema)
      .load(settings.inputDataSpec)

    // http://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex
    val preppedDf = dfZipWithIndex(
      rawDf.select(
        rawDf("timestamp"),
        lower(rawDf("hostname")).alias("hostname"),
        lower(rawDf("portname")).alias("portname"),
        rawDf("portspeed"),
        rawDf("totalrxbytes"),
        rawDf("totaltxbytes")
      )
        .orderBy(
          "hostname",
          "portname",
          "timestamp"
        )
    )

    val cookedDf = preppedDf
      .transform(deltaTime)
      .transform(deltaRxBytes)
      .transform(deltaTxBytes)
      .persist()

    println("Cooked Data:")
    cookedDf.printSchema()
    cookedDf.show(1000)

    cookedDf.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .mode("overwrite")
      .save(settings.outputDataSpec)

    logger.info("Finishing with:  " + settings.outputDataSpec)
  }

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
    // Do a self-join here
    df
  }

  def deltaRxBytes(df: DataFrame): DataFrame = {
    // FIXME
    // Do a self-join here
    df
  }

  def deltaTxBytes(df: DataFrame): DataFrame = {
    // FIXME
    // Do a self-join here
    df
  }
}
