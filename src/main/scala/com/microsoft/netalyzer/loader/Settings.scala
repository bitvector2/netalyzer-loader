package com.microsoft.netalyzer.loader

import com.typesafe.config.ConfigFactory

class Settings() {
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "netalyzer-loader")
  val rawData = config.getString("netalyzer-loader.rawData")
  val cookedData = config.getString("netalyzer-loader.cookedData")
}
