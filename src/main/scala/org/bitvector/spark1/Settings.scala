package org.bitvector.spark1

import com.typesafe.config.ConfigFactory

class Settings() {
  val config = ConfigFactory.load()

  // validate vs. reference.conf
  config.checkValid(ConfigFactory.defaultReference(), "examples")

  // non-lazy fields, we want all exceptions at construct time
  val foo = config.getString("examples.foo")
  val bar = config.getInt("examples.bar")
}
