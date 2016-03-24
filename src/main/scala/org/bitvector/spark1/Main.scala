package org.bitvector.spark1

import com.typesafe.scalalogging._
import org.apache.spark.{SparkConf, SparkContext}

object Main extends LazyLogging {
  val settings = new Settings()

  def main(args: Array[String]) = {
    logger.info("Starting with:  " + settings.foo)

    val conf = new SparkConf().setAppName("WASBIOTest")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("wasb:///HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv")

    //find the rows which have only one digit in the 7th column in the CSV
    val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

    rdd1.saveAsTextFile("wasb:///tmp/HVACout")

    sc.stop()

    logger.info("Stopping...")
  }
}
