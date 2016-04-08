package com.microsoft.spark1

import com.typesafe.config.ConfigFactory

class Settings() {
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "spark1")
  val inputDataSpec = config.getString("spark1.inputDataSpec")
  val outputDataSpec = config.getString("spark1.outputDataSpec")
}
