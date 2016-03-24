package org.bitvector.spark1

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  private val logger = org.log4s.getLogger

  def main(args: Array[String]) = {
    logger.info("Starting...")


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
