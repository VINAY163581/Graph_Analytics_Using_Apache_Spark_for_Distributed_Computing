package com.graphanalytics.jobs

import com.graphanalytics.core.GraphLoader
import com.graphanalytics.util.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TriangleCommunityJob {

  case class Config(
      edgesPath: String,
      outputPath: String,
      hasTimestamp: Boolean,
      topKHighDegree: Int,
      approxEdgeSampleFraction: Double
  )

  def run(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    val directed = GraphLoader.loadEdges(spark, config.edgesPath, config.hasTimestamp)

    val undirected = directed
      .select(
        least(col("src"), col("dst")).as("u"),
        greatest(col("src"), col("dst")).as("v")
      )
      .distinct()
      .cache()

    val degree = undirected
      .select(col("u").as("node"))
      .union(undirected.select(col("v").as("node")))
      .groupBy("node")
      .agg(count(lit(1)).as("degree"))
      .cache()

    val e1 = undirected.select(col("u").as("a"), col("v").as("b"))
    val e2 = undirected.select(col("u").as("a"), col("v").as("c"))
    val e3 = undirected.select(col("u").as("b"), col("v").as("c"))

    val triangles = e1
      .join(e2, Seq("a"))
      .filter(col("b") < col("c"))
      .join(e3, Seq("b", "c"))
      .select("a", "b", "c")
      .cache()

    val exactTriangleCount = triangles.count()

    val nodeTriangleCounts = triangles
      .select(col("a").as("node"))
      .union(triangles.select(col("b").as("node")))
      .union(triangles.select(col("c").as("node")))
      .groupBy("node")
      .agg(count(lit(1)).as("triangle_count"))

    val highDegree = degree.orderBy(desc("degree")).limit(config.topKHighDegree)

    val clustering = highDegree
      .join(nodeTriangleCounts, Seq("node"), "left")
      .na.fill(0L, Seq("triangle_count"))
      .withColumn(
        "local_clustering_coeff",
        when(col("degree") < 2, lit(0.0))
          .otherwise(col("triangle_count") / (col("degree") * (col("degree") - lit(1.0)) / lit(2.0)))
      )
      .orderBy(desc("triangle_count"))

    val sampled = undirected.sample(withReplacement = false, config.approxEdgeSampleFraction, seed = 17L)

    val s1 = sampled.select(col("u").as("a"), col("v").as("b"))
    val s2 = sampled.select(col("u").as("a"), col("v").as("c"))
    val s3 = sampled.select(col("u").as("b"), col("v").as("c"))

    val sampledTriangles = s1
      .join(s2, Seq("a"))
      .filter(col("b") < col("c"))
      .join(s3, Seq("b", "c"))

    val sampledTriangleCount = sampledTriangles.count()
    val estimate = if (config.approxEdgeSampleFraction > 0.0)
      sampledTriangleCount.toDouble / math.pow(config.approxEdgeSampleFraction, 3)
    else 0.0

    val sampledNodeTriangleCounts = sampledTriangles
      .select(col("a").as("node"))
      .union(sampledTriangles.select(col("b").as("node")))
      .union(sampledTriangles.select(col("c").as("node")))
      .groupBy("node")
      .agg(count(lit(1)).as("sampled_triangle_count"))

    val approxClustering = highDegree
      .join(sampledNodeTriangleCounts, Seq("node"), "left")
      .na.fill(0L, Seq("sampled_triangle_count"))
      .withColumn(
        "closed_pairs_estimate",
        if (config.approxEdgeSampleFraction > 0.0)
          col("sampled_triangle_count") / lit(math.pow(config.approxEdgeSampleFraction, 3))
        else lit(0.0)
      )
      .withColumn(
        "local_clustering_coeff_approx",
        when(col("degree") < 2, lit(0.0))
          .otherwise(col("closed_pairs_estimate") / (col("degree") * (col("degree") - lit(1.0)) / lit(2.0)))
      )
      .orderBy(desc("closed_pairs_estimate"))

    val summary = Seq(
      (
        undirected.count(),
        exactTriangleCount,
        sampledTriangleCount,
        config.approxEdgeSampleFraction,
        estimate
      )
    ).toDF(
      "undirected_edge_count",
      "exact_triangle_count",
      "sampled_triangle_count",
      "edge_sample_fraction",
      "approx_triangle_estimate"
    )

    IOUtils.writeCsv(clustering, s"${config.outputPath}/triangle_high_degree_clustering")
    IOUtils.writeCsv(approxClustering, s"${config.outputPath}/triangle_high_degree_clustering_approx")
    IOUtils.writeCsv(summary, s"${config.outputPath}/triangle_summary")
  }
}
