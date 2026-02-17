package com.graphanalytics.jobs

import com.graphanalytics.core.GraphLoader
import com.graphanalytics.util.{IOUtils, TimeUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SkewBenchmarkJob {

  case class BenchmarkRow(
      scale_fraction: Double,
      edge_count: Long,
      heavy_edge_ratio: Double,
      baseline_runtime_ms: Long,
      salted_runtime_ms: Long,
      speedup: Double
  )

  case class Config(
      edgesPath: String,
      outputPath: String,
      hasTimestamp: Boolean,
      heavyKeyCount: Int,
      saltBuckets: Int,
      scaleFractions: Seq[Double]
  )

  def run(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    val baseEdges = GraphLoader.loadEdges(spark, config.edgesPath, config.hasTimestamp).select("src", "dst").cache()

    val rows = config.scaleFractions.map { fraction =>
      val edges = if (fraction >= 0.999) baseEdges else baseEdges.sample(withReplacement = false, fraction, 11L)
      val edgeCount = edges.count()

      val nodes = GraphLoader.deriveNodes(edges).cache()
      val n = math.max(nodes.count(), 1L).toDouble

      val ranks = nodes.select(col("node").as("src"), lit(1.0 / n).as("rank")).cache()

      val degree = edges.groupBy("src").agg(count(lit(1)).as("degree")).cache()
      val heavy = degree.orderBy(desc("degree")).limit(config.heavyKeyCount).select("src").cache()

      val heavyStats = degree
        .join(heavy.withColumn("is_heavy", lit(1)), Seq("src"), "left")
        .agg(
          sum(when(col("is_heavy") === 1, col("degree")).otherwise(lit(0L))).as("heavy_edges"),
          sum(col("degree")).as("all_edges")
        )
        .select((col("heavy_edges") / col("all_edges")).as("heavy_edge_ratio"))
        .as[Double]
        .take(1)
        .headOption
        .getOrElse(0.0)

      val (_, baselineMs) = TimeUtils.timed {
        edges
          .join(ranks, Seq("src"), "left")
          .groupBy("dst")
          .agg(sum(col("rank")).as("score"))
          .count()
      }

      val edgesTagged = edges.join(heavy.withColumn("is_heavy", lit(1)), Seq("src"), "left")
      val edgesSalted = edgesTagged
        .withColumn(
          "salt",
          when(
            col("is_heavy") === 1,
            pmod(xxhash64(col("src"), col("dst")), lit(config.saltBuckets))
          ).otherwise(lit(0))
        )
        .drop("is_heavy")

      val ranksTagged = ranks.join(heavy.withColumn("is_heavy", lit(1)), Seq("src"), "left")
      val ranksHeavy = ranksTagged
        .filter(col("is_heavy") === 1)
        .withColumn("salt", explode(sequence(lit(0), lit(config.saltBuckets - 1))))
        .drop("is_heavy")
      val ranksNormal = ranksTagged
        .filter(col("is_heavy").isNull)
        .withColumn("salt", lit(0))
        .drop("is_heavy")
      val ranksSalted = ranksHeavy.unionByName(ranksNormal)

      val (_, saltedMs) = TimeUtils.timed {
        edgesSalted
          .join(ranksSalted, Seq("src", "salt"), "left")
          .groupBy("dst")
          .agg(sum(col("rank")).as("score"))
          .count()
      }

      nodes.unpersist()
      ranks.unpersist()
      degree.unpersist()
      heavy.unpersist()

      BenchmarkRow(
        scale_fraction = fraction,
        edge_count = edgeCount,
        heavy_edge_ratio = heavyStats,
        baseline_runtime_ms = baselineMs,
        salted_runtime_ms = saltedMs,
        speedup = if (saltedMs > 0) baselineMs.toDouble / saltedMs else 0.0
      )
    }

    val summary = rows.toDF

    IOUtils.writeCsv(summary.orderBy("scale_fraction"), s"${config.outputPath}/skew_scaling_summary")
  }
}
