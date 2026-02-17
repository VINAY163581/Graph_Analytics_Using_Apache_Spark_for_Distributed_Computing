package com.graphanalytics.jobs

import com.graphanalytics.core.{GraphLoader, PageRankCore}
import com.graphanalytics.util.{IOUtils, TimeUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PageRankJob {

  case class Config(
      edgesPath: String,
      nodesPath: Option[String],
      outputPath: String,
      iterations: Int,
      damping: Double,
      hasTimestamp: Boolean,
      topN: Int
  )

  def run(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    val edges = GraphLoader.loadEdges(spark, config.edgesPath, config.hasTimestamp).cache()
    val nodes = config.nodesPath.map(GraphLoader.loadNodes(spark, _)).getOrElse(GraphLoader.deriveNodes(edges)).cache()

    val (result, runtimeMs) = TimeUtils.timed {
      PageRankCore.run(spark, nodes, edges.select("src", "dst"), config.iterations, config.damping)
    }

    val topRanks = result.ranks.orderBy(desc("rank")).limit(config.topN)

    val summary = Seq(
      (
        nodes.count(),
        edges.count(),
        config.iterations,
        config.damping,
        runtimeMs
      )
    ).toDF("node_count", "edge_count", "iterations", "damping", "runtime_ms")

    IOUtils.writeCsv(topRanks, s"${config.outputPath}/pagerank_top")
    IOUtils.writeCsv(result.convergence, s"${config.outputPath}/pagerank_convergence")
    IOUtils.writeCsv(summary, s"${config.outputPath}/pagerank_summary")
  }
}
