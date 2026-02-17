ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val sparkVersion = "3.5.2"

lazy val root = (project in file("."))
  .settings(
    name := "spark-graph-analytics",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "com.github.scopt" %% "scopt" % "4.1.0",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    Compile / mainClass := Some("GraphAnalytics"),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Djava.security.manager=allow",
      "-Dlog4j2.configurationFile=src/main/resources/log4j2.properties",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / fork := true
  )
