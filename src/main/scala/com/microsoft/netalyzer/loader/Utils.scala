package com.microsoft.netalyzer.loader


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

object Utils {

  def initializeDb(path: String, sc: SQLContext): Unit = {
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
          id VARCHAR(127)
        )
        CLUSTERED BY(id) INTO 16 BUCKETS
        STORED AS ORC
        TBLPROPERTIES("transactional"="true")
      """.stripMargin
    )
  }

  // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
  def importCsvData(path: String, sc: SQLContext): Unit = {
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

    val fileSystem = FileSystem.get(sc.sparkContext.hadoopConfiguration)
    val tmpPath = path + "_LOADING"

    if (fileSystem.exists(new Path(tmpPath))) {
      val rawDf = sc.read
        .format("com.databricks.spark.csv")
        .option("mode", "FAILFAST")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(customSchema)
        .load(tmpPath)
        .repartition(16)

      rawDf.registerTempTable("rawDf")

      val newDf = sc.sql(
        """
          SELECT datetime,
          hostname,
          portname,
          portspeed,
          totalrxbytes,
          totaltxbytes,
          sha2(concat(datetime, hostname, portname), 256) AS id
          FROM rawDf
        """.stripMargin
      )

      newDf.printSchema()
      newDf.show(100)
      newDf.write.mode("append").saveAsTable("netalyzer.samples")
      fileSystem.delete(new Path(tmpPath), true)
    }
    else if (fileSystem.exists(new Path(path))) {
      fileSystem.rename(new Path(path), new Path(tmpPath))

      val rawDf = sc.read
        .format("com.databricks.spark.csv")
        .option("mode", "FAILFAST")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(customSchema)
        .load(tmpPath)
        .repartition(16)

      rawDf.registerTempTable("rawDf")

      val newDf = sc.sql(
        """
          SELECT datetime,
          hostname,
          portname,
          portspeed,
          totalrxbytes,
          totaltxbytes,
          sha2(concat(datetime, hostname, portname), 256) AS id
          FROM rawDf
        """.stripMargin
      )

      newDf.printSchema()
      newDf.show(100)
      newDf.write.mode("append").saveAsTable("netalyzer.samples")
      fileSystem.delete(new Path(tmpPath), true)
    }
  }

}
