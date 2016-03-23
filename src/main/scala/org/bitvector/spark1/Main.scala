package org.bitvector.spark1

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Main {
  def main(args: Array[String]) = {
    val logger = LoggerFactory.getLogger(this.getClass.getName)
    logger.info("Starting...")

    val conf = new SparkConf().setAppName("WASBIOTest")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("wasb:///HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv")

    //find the rows which have only one digit in the 7th column in the CSV
    val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

    rdd1.saveAsTextFile("wasb:///tmp/HVACout")

    logger.info("Finished...")
  }
}
