package com.graphanalytics.jobs

import com.graphanalytics.core.{GraphLoader, PageRankCore}
import com.graphanalytics.util.IOUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TemporalPageRankJob {

  case class Config(
      edgesPath: String,
      outputPath: String,
      iterations: Int,
      damping: Double
  )

  def run(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    val edges = GraphLoader.loadEdges(spark, config.edgesPath, hasTimestamp = true)
      .filter(col("timestamp").isNotNull)
      .withColumn("day", to_date(col("timestamp")))
      .cache()

    val days = edges
      .select(date_format(col("day"), "yyyy-MM-dd").as("day_str"))
      .distinct()
      .orderBy("day_str")
      .as[String]
      .collect()
      .toSeq

    val perDayRanks = days.map { day =>
      val dayEdges = edges
        .filter(date_format(col("day"), "yyyy-MM-dd") === lit(day))
        .select("src", "dst")
      val nodes = GraphLoader.deriveNodes(dayEdges)

      if (nodes.head(1).nonEmpty) {
        val result = PageRankCore.run(spark, nodes, dayEdges, config.iterations, config.damping)
        result.ranks.withColumn("day", lit(day))
      } else {
        spark.emptyDataFrame
      }
    }.filter(_.columns.nonEmpty)

    val ranksByDay = if (perDayRanks.nonEmpty) {
      perDayRanks.reduce(_ unionByName _).cache()
    } else {
      spark.emptyDataFrame
    }

    if (ranksByDay.columns.nonEmpty) {
      val nodeWindow = Window.partitionBy("node").orderBy("day")

      val withDelta = ranksByDay
        .withColumn("prev_rank", lag(col("rank"), 1).over(nodeWindow))
        .withColumn("abs_delta", abs(col("rank") - coalesce(col("prev_rank"), col("rank"))))

      val volatility = withDelta
        .groupBy("node")
        .agg(
          stddev_samp(col("rank")).as("rank_volatility"),
          max(col("rank")).as("max_rank"),
          min(col("rank")).as("min_rank"),
          (max(col("rank")) - min(col("rank"))).as("rank_range"),
          count(lit(1)).as("window_count")
        )
        .na.fill(0.0, Seq("rank_volatility", "rank_range"))

      val volatilityRanked = volatility.orderBy(desc("rank_volatility"), desc("rank_range"))

      IOUtils.writeCsv(ranksByDay.orderBy(col("day"), desc("rank")), s"${config.outputPath}/temporal_pagerank_by_day")
      IOUtils.writeCsv(volatilityRanked, s"${config.outputPath}/temporal_volatility")
    } else {
      val empty = Seq(("no-data", 0.0)).toDF("status", "value")
      IOUtils.writeCsv(empty, s"${config.outputPath}/temporal_volatility")
    }
  }
}
