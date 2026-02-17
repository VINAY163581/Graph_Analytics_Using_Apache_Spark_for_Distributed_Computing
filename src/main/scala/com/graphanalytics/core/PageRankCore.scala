package com.graphanalytics.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PageRankCore {

  case class PageRankResult(ranks: DataFrame, convergence: DataFrame)

  def run(
      spark: SparkSession,
      nodes: DataFrame,
      edges: DataFrame,
      iterations: Int,
      damping: Double
  ): PageRankResult = {
    import spark.implicits._

    val n = nodes.count().toDouble
    require(n > 0, "Node count must be > 0")

    val baseRank = (1.0 - damping) / n

    val outDegree = edges.groupBy(col("src")).agg(count(lit(1)).as("out_degree")).cache()

    var ranks = nodes.select(col("node"), lit(1.0 / n).as("rank")).cache()

    val metrics = scala.collection.mutable.ArrayBuffer[(Int, Double, Long)]()

    (1 to iterations).foreach { i =>
      val iterStart = System.nanoTime()

      val contribs = edges
        .join(ranks, edges("src") === ranks("node"), "left")
        .join(outDegree, Seq("src"), "left")
        .select(
          edges("dst").as("node"),
          (coalesce(ranks("rank"), lit(0.0)) / coalesce(col("out_degree"), lit(1))).as("contrib")
        )

      val updated = nodes
        .join(contribs.groupBy(col("node")).agg(sum(col("contrib")).as("sum_contrib")), Seq("node"), "left")
        .select(
          col("node"),
          (lit(baseRank) + lit(damping) * coalesce(col("sum_contrib"), lit(0.0))).as("rank")
        )
        .cache()

      val delta = ranks
        .join(updated, Seq("node"))
        .select(abs(ranks("rank") - updated("rank")).as("diff"))
        .agg(sum(col("diff")))
        .as[Double]
        .head()

      val elapsedMs = (System.nanoTime() - iterStart) / 1000000L
      metrics += ((i, delta, elapsedMs))

      ranks.unpersist()
      ranks = updated
    }

    val convergenceDf = metrics.toSeq.toDF("iteration", "l1_delta", "iteration_runtime_ms")
    PageRankResult(ranks, convergenceDf)
  }
}
