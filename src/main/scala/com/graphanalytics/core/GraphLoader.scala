package com.graphanalytics.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GraphLoader {
  def loadEdges(spark: SparkSession, path: String, hasTimestamp: Boolean): DataFrame = {
    val base = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    val standardized =
      if (hasTimestamp && base.columns.contains("timestamp")) {
        base.select(
          col("src").cast("string").as("src"),
          col("dst").cast("string").as("dst"),
          to_timestamp(col("timestamp")).as("timestamp")
        )
      } else {
        base.select(
          col("src").cast("string").as("src"),
          col("dst").cast("string").as("dst")
        )
      }

    standardized
      .filter(col("src").isNotNull && col("dst").isNotNull)
      .filter(col("src") =!= col("dst"))
  }

  def loadNodes(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select(col("node_id").cast("string").as("node"))
      .filter(col("node").isNotNull)
      .distinct()
  }

  def deriveNodes(edges: DataFrame): DataFrame = {
    edges
      .select(col("src").as("node"))
      .union(edges.select(col("dst").as("node")))
      .distinct()
  }
}
