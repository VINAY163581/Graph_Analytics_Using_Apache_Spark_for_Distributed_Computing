package com.graphanalytics.core

import org.apache.spark.sql.SparkSession

object SparkFactory {
  def session(appName: String): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "200")

    val spark =
      if (sys.env.get("SPARK_MASTER").isDefined) {
        builder.master(sys.env("SPARK_MASTER")).getOrCreate()
      } else {
        builder.master("local[*]").getOrCreate()
      }

    // Keep local runs readable; avoids huge task-level INFO spam in sbt output.
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
