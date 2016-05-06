package com.microsoft.netalyzer.loader

import com.typesafe.config.ConfigFactory

class Settings() {
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "netalyzer-loader")
  val inputDataSpec = config.getString("netalyzer-loader.inputDataSpec")
  val outputDataSpec = config.getString("netalyzer-loader.outputDataSpec")
}
