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
        StructField("totalrxbytes", LongType, nullable = false),
        StructField("totaltxbytes", LongType, nullable = false)
      )
    )

    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    val rawDf = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
      .schema(customSchema)
      .load(settings.inputDataSpec)

    val cookedDf = rawDf
      .transform(addDeltas)
      .persist()

    println("Cooked Data Sample:")
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
    val newdf = sqlContext.sql("select timestamp, hostname, portname, portspeed, totalrxbytes, totaltxbytes, " +
      "unix_timestamp(timestamp) - lag(unix_timestamp(timestamp)) over (partition by hostname, portname order by timestamp) as deltaseconds, " +
      "totalrxbytes - lag(totalrxbytes) over (partition by hostname, portname order by timestamp) as deltarxbytes, " +
      "totaltxbytes - lag(totaltxbytes) over (partition by hostname, portname order by timestamp) as deltatxbytes " +
      "from df " +
      "order by hostname, portname, timestamp")
    sqlContext.dropTempTable("df")
    newdf
  }


}
