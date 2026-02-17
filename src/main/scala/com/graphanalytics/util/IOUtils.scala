package com.graphanalytics.util

import org.apache.spark.sql.{DataFrame, SaveMode}

object IOUtils {
  def writeCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }
}
