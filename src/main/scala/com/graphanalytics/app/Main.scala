package com.graphanalytics.app

import com.graphanalytics.core.SparkFactory
import com.graphanalytics.jobs.{PageRankJob, SkewBenchmarkJob, TemporalPageRankJob, TriangleCommunityJob}

object Main {

  sealed trait Command
  case class PageRankCmd(config: PageRankJob.Config) extends Command
  case class TriangleCmd(config: TriangleCommunityJob.Config) extends Command
  case class TemporalCmd(config: TemporalPageRankJob.Config) extends Command
  case class SkewCmd(config: SkewBenchmarkJob.Config) extends Command

  case class CliConfig(command: Option[Command] = None)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[CliConfig]("graph-analytics") {
      head("Graph Analytics Using Apache Spark", "0.1.0")

      cmd("pagerank")
        .action((_, c) => c.copy(command = Some(PageRankCmd(PageRankJob.Config("", None, "", 10, 0.85, false, 20)))))
        .text("Run iterative PageRank")
        .children(
          opt[String]("edges").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(edgesPath = x))))
          }),
          opt[String]("nodes").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(nodesPath = Some(x)))))
          }),
          opt[String]("output").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(outputPath = x))))
          }),
          opt[Int]("iterations").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(iterations = x))))
          }),
          opt[Double]("damping").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(damping = x))))
          }),
          opt[Unit]("has-timestamp").optional().action((_, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(hasTimestamp = true))))
          }),
          opt[Int]("top-n").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[PageRankCmd].config
            c.copy(command = Some(PageRankCmd(current.copy(topN = x))))
          })
        )

      cmd("triangle")
        .action((_, c) => c.copy(command = Some(TriangleCmd(TriangleCommunityJob.Config("", "", false, 50, 0.3)))))
        .text("Run triangle/community signal analysis")
        .children(
          opt[String]("edges").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[TriangleCmd].config
            c.copy(command = Some(TriangleCmd(current.copy(edgesPath = x))))
          }),
          opt[String]("output").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[TriangleCmd].config
            c.copy(command = Some(TriangleCmd(current.copy(outputPath = x))))
          }),
          opt[Unit]("has-timestamp").optional().action((_, c) => {
            val current = c.command.get.asInstanceOf[TriangleCmd].config
            c.copy(command = Some(TriangleCmd(current.copy(hasTimestamp = true))))
          }),
          opt[Int]("top-k").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[TriangleCmd].config
            c.copy(command = Some(TriangleCmd(current.copy(topKHighDegree = x))))
          }),
          opt[Double]("sample-fraction").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[TriangleCmd].config
            c.copy(command = Some(TriangleCmd(current.copy(approxEdgeSampleFraction = x))))
          })
        )

      cmd("temporal")
        .action((_, c) => c.copy(command = Some(TemporalCmd(TemporalPageRankJob.Config("", "", 10, 0.85)))))
        .text("Run temporal PageRank and volatility analytics")
        .children(
          opt[String]("edges").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[TemporalCmd].config
            c.copy(command = Some(TemporalCmd(current.copy(edgesPath = x))))
          }),
          opt[String]("output").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[TemporalCmd].config
            c.copy(command = Some(TemporalCmd(current.copy(outputPath = x))))
          }),
          opt[Int]("iterations").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[TemporalCmd].config
            c.copy(command = Some(TemporalCmd(current.copy(iterations = x))))
          }),
          opt[Double]("damping").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[TemporalCmd].config
            c.copy(command = Some(TemporalCmd(current.copy(damping = x))))
          })
        )

      cmd("skew")
        .action((_, c) => c.copy(command = Some(SkewCmd(SkewBenchmarkJob.Config("", "", false, 20, 8, Seq(0.1, 0.3, 0.6, 1.0))))))
        .text("Run skew mitigation and scaling benchmark")
        .children(
          opt[String]("edges").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(edgesPath = x))))
          }),
          opt[String]("output").required().action((x, c) => {
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(outputPath = x))))
          }),
          opt[Unit]("has-timestamp").optional().action((_, c) => {
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(hasTimestamp = true))))
          }),
          opt[Int]("heavy-keys").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(heavyKeyCount = x))))
          }),
          opt[Int]("salt-buckets").optional().action((x, c) => {
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(saltBuckets = x))))
          }),
          opt[String]("scale-fractions").optional().action((x, c) => {
            val fractions = x.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map(_.toDouble)
            val current = c.command.get.asInstanceOf[SkewCmd].config
            c.copy(command = Some(SkewCmd(current.copy(scaleFractions = fractions))))
          })
        )
    }

    parser.parse(args, CliConfig()) match {
      case Some(CliConfig(Some(command))) =>
        val spark = SparkFactory.session("GraphAnalyticsSpark")
        try {
          command match {
            case PageRankCmd(config) => PageRankJob.run(spark, config)
            case TriangleCmd(config) => TriangleCommunityJob.run(spark, config)
            case TemporalCmd(config) => TemporalPageRankJob.run(spark, config)
            case SkewCmd(config) => SkewBenchmarkJob.run(spark, config)
          }
        } finally {
          spark.stop()
        }
      case _ =>
        if (args.isEmpty) {
          println(
            "No command provided. Use one of: pagerank | triangle | temporal | skew"
          )
          println("Example:")
          println(
            "  sbt \"run pagerank --edges data/sample/edges.csv --nodes data/sample/nodes.csv --output output/local --iterations 12 --damping 0.85 --has-timestamp --top-n 25\""
          )
          System.exit(0)
        } else {
          System.exit(1)
        }
    }
  }
}
