package org.bitvector.spark1

import com.typesafe.config.ConfigFactory

class Settings() {
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "examples")
  val foo = config.getString("examples.foo")
  val bar = config.getInt("examples.bar")
}
