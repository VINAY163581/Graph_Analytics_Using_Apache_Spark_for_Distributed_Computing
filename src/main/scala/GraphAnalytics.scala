import com.graphanalytics.core.{GraphLoader, SparkFactory}
import com.graphanalytics.jobs.{PageRankJob, SkewBenchmarkJob, TemporalPageRankJob, TriangleCommunityJob}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import scala.util.Random

object GraphAnalytics {

  case class Config(
      edges: String = "data/sample/edges.csv",
      nodes: String = "data/sample/nodes.csv",
      output: String = "output",
      iterations: Int = 10,
      damping: Double = 0.85,
      runCommunity: Boolean = true,
      runTemporal: Boolean = true,
      runScale: Boolean = true,
      runSkew: Boolean = true,
      topN: Int = 20,
      topK: Int = 20,
      sampleFraction: Double = 0.25,
      heavyKeys: Int = 10,
      saltBuckets: Int = 12,
      scaleFractions: String = "0.3,0.6,1.0"
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("GraphAnalytics") {
      head("Graph Analytics", "0.1.0")

      opt[String]("edges").optional().action((x, c) => c.copy(edges = x))
      opt[String]("nodes").optional().action((x, c) => c.copy(nodes = x))
      opt[String]("output").optional().action((x, c) => c.copy(output = x))
      opt[Int]("iterations").optional().action((x, c) => c.copy(iterations = x))
      opt[Double]("damping").optional().action((x, c) => c.copy(damping = x))
      opt[Boolean]("runCommunity").optional().action((x, c) => c.copy(runCommunity = x))
      opt[Boolean]("runTemporal").optional().action((x, c) => c.copy(runTemporal = x))
      opt[Boolean]("runScale").optional().action((x, c) => c.copy(runScale = x))
      opt[Boolean]("runSkew").optional().action((x, c) => c.copy(runSkew = x))
      opt[Int]("topN").optional().action((x, c) => c.copy(topN = x))
      opt[Int]("topK").optional().action((x, c) => c.copy(topK = x))
      opt[Double]("sampleFraction").optional().action((x, c) => c.copy(sampleFraction = x))
      opt[Int]("heavyKeys").optional().action((x, c) => c.copy(heavyKeys = x))
      opt[Int]("saltBuckets").optional().action((x, c) => c.copy(saltBuckets = x))
      opt[String]("scaleFractions").optional().action((x, c) => c.copy(scaleFractions = x))
    }

    parser.parse(args, Config()) match {
      case Some(config) => runAll(config)
      case None => System.exit(1)
    }
  }

  private def runAll(config: Config): Unit = {
    ensureInputData(config)

    val spark = SparkFactory.session("GraphAnalytics")
    import spark.implicits._

    try {
      val edges = GraphLoader.loadEdges(spark, config.edges, hasTimestamp = true).select("src", "dst", "timestamp").cache()
      val nodes =
        if (Files.exists(Paths.get(config.nodes))) GraphLoader.loadNodes(spark, config.nodes)
        else GraphLoader.deriveNodes(edges)

      val nodeCount = nodes.count()
      val edgeCount = edges.count()
      println(s"Graph loaded: nodes=$nodeCount, edges=$edgeCount")

      PageRankJob.run(
        spark,
        PageRankJob.Config(
          edgesPath = config.edges,
          nodesPath = if (Files.exists(Paths.get(config.nodes))) Some(config.nodes) else None,
          outputPath = config.output,
          iterations = config.iterations,
          damping = config.damping,
          hasTimestamp = true,
          topN = config.topN
        )
      )

      val prConvergence = readCsv(spark, s"${config.output}/pagerank_convergence")
        .withColumn("phase", lit("pagerank_baseline"))
        .withColumn("graphNodes", lit(nodeCount))
        .withColumn("graphEdges", lit(edgeCount))
        .select("phase", "graphNodes", "graphEdges", "iteration", "iteration_runtime_ms", "l1_delta")
        .withColumnRenamed("iteration_runtime_ms", "runtimeMs")
        .withColumnRenamed("l1_delta", "l1Delta")

      println("===== PAGERANK CONVERGENCE =====")
      prConvergence.show(config.iterations, truncate = false)

      println("===== TOP PAGERANK NODES =====")
      readCsv(spark, s"${config.output}/pagerank_top")
        .withColumnRenamed("node", "id")
        .orderBy(desc("rank"))
        .show(config.topN, truncate = false)

      if (config.runCommunity) {
        TriangleCommunityJob.run(
          spark,
          TriangleCommunityJob.Config(
            edgesPath = config.edges,
            outputPath = config.output,
            hasTimestamp = true,
            topKHighDegree = config.topK,
            approxEdgeSampleFraction = config.sampleFraction
          )
        )

        println("===== COMMUNITY SIGNALS: EXACT =====")
        readCsv(spark, s"${config.output}/triangle_high_degree_clustering")
          .withColumnRenamed("triangle_count", "closed_pairs")
          .withColumn("possible_pairs", col("degree") * (col("degree") - lit(1.0)) / lit(2.0))
          .withColumnRenamed("local_clustering_coeff", "clustering_coeff_exact")
          .select("node", "degree", "closed_pairs", "possible_pairs", "clustering_coeff_exact")
          .show(config.topK, truncate = false)

        println("===== COMMUNITY SIGNALS: APPROX =====")
        readCsv(spark, s"${config.output}/triangle_high_degree_clustering_approx")
          .withColumnRenamed("sampled_triangle_count", "sampled_closed")
          .withColumn("possible_pairs", col("degree") * (col("degree") - lit(1.0)) / lit(2.0))
          .withColumnRenamed("local_clustering_coeff_approx", "clustering_coeff_approx")
          .select("node", "degree", "sampled_closed", "possible_pairs", "closed_pairs_estimate", "clustering_coeff_approx")
          .show(config.topK, truncate = false)
      }

      if (config.runTemporal) {
        TemporalPageRankJob.run(
          spark,
          TemporalPageRankJob.Config(
            edgesPath = config.edges,
            outputPath = config.output,
            iterations = math.max(5, config.iterations / 2),
            damping = config.damping
          )
        )

        println("===== TEMPORAL RANK VOLATILITY =====")
        readCsv(spark, s"${config.output}/temporal_volatility")
          .withColumnRenamed("node", "id")
          .select("id", "rank_volatility", "max_rank", "min_rank", "rank_range")
          .orderBy(desc("rank_volatility"))
          .show(20, truncate = false)
      }

      if (config.runScale || config.runSkew) {
        val fractions = config.scaleFractions.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map(_.toDouble)

        SkewBenchmarkJob.run(
          spark,
          SkewBenchmarkJob.Config(
            edgesPath = config.edges,
            outputPath = config.output,
            hasTimestamp = true,
            heavyKeyCount = config.heavyKeys,
            saltBuckets = config.saltBuckets,
            scaleFractions = fractions
          )
        )

        val skewSummary = readCsv(spark, s"${config.output}/skew_scaling_summary")
        val finalDelta = prConvergence
          .orderBy(desc("iteration"))
          .select("l1Delta")
          .as[Double]
          .take(1)
          .headOption
          .getOrElse(0.0)

        if (config.runScale) {
          println("===== SCALE STUDY =====")
          skewSummary
            .withColumn("graphNodes", round(lit(nodeCount.toDouble) * col("scale_fraction")).cast("long"))
            .withColumnRenamed("edge_count", "graphEdges")
            .withColumn("iterations", lit(config.iterations))
            .withColumnRenamed("baseline_runtime_ms", "runtimeMs")
            .withColumn("finalDelta", lit(finalDelta))
            .select("graphNodes", "graphEdges", "iterations", "runtimeMs", "finalDelta")
            .orderBy("graphEdges")
            .show(50, truncate = false)
        }

        if (config.runSkew) {
          println("===== SKEW MITIGATION =====")
          val fullScale = skewSummary.orderBy(desc("scale_fraction")).limit(1)

          val baseline = fullScale
            .select(
              lit("baseline").as("strategy"),
              lit(nodeCount).as("graphNodes"),
              col("edge_count").as("graphEdges"),
              lit(config.iterations).as("iterations"),
              col("baseline_runtime_ms").as("totalRuntimeMs")
            )

          val salted = fullScale
            .select(
              lit("salted_join").as("strategy"),
              lit(nodeCount).as("graphNodes"),
              col("edge_count").as("graphEdges"),
              lit(config.iterations).as("iterations"),
              col("salted_runtime_ms").as("totalRuntimeMs")
            )

          baseline.unionByName(salted).show(truncate = false)
        }
      }

      println(s"Results written to: ${config.output}")
    } finally {
      spark.stop()
    }
  }

  private def readCsv(spark: SparkSession, path: String) = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  }

  private def ensureInputData(config: Config): Unit = {
    val edgesPath = Paths.get(config.edges)
    val nodesPath = Paths.get(config.nodes)

    if (Files.exists(edgesPath) && Files.exists(nodesPath)) return

    if (edgesPath.getParent != null) Files.createDirectories(edgesPath.getParent)
    if (nodesPath.getParent != null) Files.createDirectories(nodesPath.getParent)

    val nodeCount = 10000
    val edgeCount = 100000
    val days = 10
    val perDay = edgeCount / days
    val rnd = new Random(17L)

    val nodesWriter = new BufferedWriter(new FileWriter(nodesPath.toFile))
    val edgesWriter = new BufferedWriter(new FileWriter(edgesPath.toFile))
    try {
      nodesWriter.write("node_id,type,region\n")
      (0 until nodeCount).foreach { n =>
        val region = (n % 5) + 1
        nodesWriter.write(s"$n,user,r$region\n")
      }

      edgesWriter.write("src,dst,timestamp\n")
      (1 to days).foreach { day =>
        val date = f"2026-01-$day%02d 00:00:00"

        (0 until 500).foreach { i =>
          val src = i % 20
          var dst = rnd.nextInt(nodeCount)
          while (dst == src) dst = rnd.nextInt(nodeCount)
          edgesWriter.write(s"$src,$dst,$date\n")
        }

        (0 until perDay).foreach { _ =>
          val src = rnd.nextInt(nodeCount)
          var dst = rnd.nextInt(nodeCount)
          while (dst == src) dst = rnd.nextInt(nodeCount)
          edgesWriter.write(s"$src,$dst,$date\n")
        }
      }
    } finally {
      nodesWriter.close()
      edgesWriter.close()
    }

    println(s"Generated sample graph data at: ${config.edges} and ${config.nodes}")
  }
}
