# Graph Analytics Using Apache Spark for Distributed Computing

## 1. Project Overview

This project is a distributed graph analytics pipeline built using Apache Spark and Scala.
It analyzes directed graph data to produce insights about:

- Node influence (PageRank)
- Community structure (triangle and clustering signals)
- Temporal rank behavior (day-wise PageRank and volatility)
- Performance under data skew (baseline vs salted join strategy)

The project is designed to run both:

- Locally on a laptop (for demo and evaluation)
- On larger Spark environments (for bigger datasets)



---

## 2. What Problem This Project Solves

Large graphs (social, communication, web, interaction graphs) are expensive to analyze on a single machine.
This project demonstrates distributed graph processing using Spark by solving four practical analytics tasks:

1. Ranking important nodes with iterative PageRank
2. Detecting local community strength through triangles and local clustering coefficient
3. Measuring how node importance changes over time using temporal PageRank
4. Benchmarking skew-mitigation performance using salted joins for heavy keys

---

## 3. Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.2
- SBT (build and run)
- Python 3 (for HTML dashboard generation)

---

## 4. Repository Structure

```text
.
├── build.sbt
├── conf/
│   └── pagerank-example.conf
├── data/
│   ├── edges.csv
│   └── nodes.csv
├── scripts/
│   ├── generate_sample_data.sh
│   ├── run_all_local.sh
│   └── generate_dashboard.py
├── src/main/scala/
│   ├── GraphAnalytics.scala
│   └── com/graphanalytics/
│       ├── app/Main.scala
│       ├── core/
│       ├── jobs/
│       └── util/
└── output/
```

---

## 5. Input Data Format

### 5.1 edges.csv
Expected columns:

- src
- dst
- timestamp (required for temporal analytics format like YYYY-MM-DD HH:mm:ss)

### 5.2 nodes.csv
Expected columns:

- node_id
- type (optional metadata)
- region (optional metadata)

Notes:

- src and dst are treated as string node IDs.
- Self-loops are removed during load.
- If nodes.csv is not provided in some flows, nodes can be derived from edges.

---

## 6. Analytics Modules and Outputs

### 6.1 PageRank
Computes iterative PageRank and exports:

- output/.../pagerank_top
- output/.../pagerank_convergence
- output/.../pagerank_summary

### 6.2 Triangle and Community Signals
Computes exact and approximate triangle-based local community metrics:

- output/.../triangle_high_degree_clustering
- output/.../triangle_high_degree_clustering_approx
- output/.../triangle_summary

### 6.3 Temporal PageRank
Runs PageRank by day and computes rank volatility:

- output/.../temporal_pagerank_by_day
- output/.../temporal_volatility

### 6.4 Skew Benchmark
Compares baseline join vs salted join under scaling fractions:

- output/.../skew_scaling_summary

### 6.5 Dashboard
Generates a visual HTML dashboard from CSV outputs:

- output/.../dashboard/index.html

---

## 7. Prerequisites

Install the following before running:

1. Java (recommended: Java 11 or Java 17)
2. SBT 1.x
3. Python 3 (for dashboard script)

Check tools:

```bash
java -version
sbt --version
python3 --version
```

If Spark initialization issues occur on newer Java versions, switch to Java 11.

---

## 8. How to Run the Project (Local, from GitHub Clone)

### Step 1: Clone and enter repository

```bash
git clone https://github.com/VINAY163581/Graph_Analytics_Using_Apache_Spark_for_Distributed_Computing.git
cd Graph_Analytics_Using_Apache_Spark_for_Distributed_Computing
```

### Step 2: (Optional) Generate sample data

If you want fresh synthetic data in data/sample:

```bash
chmod +x scripts/generate_sample_data.sh
./scripts/generate_sample_data.sh data/sample
```

This creates:

- data/sample/edges.csv
- data/sample/nodes.csv

### Step 3: Run all modules (recommended for evaluation)

Use explicit main class with subcommands:

```bash
sbt "runMain com.graphanalytics.app.Main pagerank --edges data/edges.csv --nodes data/nodes.csv --output output/local --iterations 12 --damping 0.85 --has-timestamp --top-n 25"
sbt "runMain com.graphanalytics.app.Main triangle --edges data/edges.csv --output output/local --has-timestamp --top-k 40 --sample-fraction 0.25"
sbt "runMain com.graphanalytics.app.Main temporal --edges data/edges.csv --output output/local --iterations 8 --damping 0.85"
sbt "runMain com.graphanalytics.app.Main skew --edges data/edges.csv --output output/local --has-timestamp --heavy-keys 10 --salt-buckets 12 --scale-fractions 0.1,0.25,0.5,0.75,1.0"
```

### Step 4: Generate dashboard

```bash
python3 scripts/generate_dashboard.py --input output/local --output output/local/dashboard
```

Open dashboard:

```bash
open output/local/dashboard/index.html
```

---

## 9. Quick One-Command Alternative

This project also includes an all-in-one runner:

```bash
sbt "runMain GraphAnalytics --edges data/edges.csv --nodes data/nodes.csv --output output/local --iterations 12 --damping 0.85 --runCommunity true --runTemporal true --runScale true --runSkew true --topN 25 --topK 40 --sampleFraction 0.25 --heavyKeys 10 --saltBuckets 12 --scaleFractions 0.1,0.25,0.5,0.75,1.0"
```

Use this one to run all analyses.

---

## 12. How to Run the Project on Dataproc (Recommended for Distributed Execution)

This section explains how your professor can run the same project on Google Cloud Dataproc using the GitHub code.

### Step 1: Build assembly JAR

```bash
sbt clean assembly
```

Expected output (example):

- target/scala-2.12/spark-graph-analytics-assembly-0.1.0.jar

### Step 2: Upload JAR and input files to GCS

```bash
gsutil cp target/scala-2.12/*assembly*.jar gs://<your-bucket>/jars/
gsutil cp data/edges.csv gs://<your-bucket>/data/
gsutil cp data/nodes.csv gs://<your-bucket>/data/
```

### Step 3: Submit Dataproc jobs (module-wise)

Run each module using the same CLI from the local version, but with GCS paths.

PageRank:

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.graphanalytics.app.Main \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.yarn.appMasterEnv.SPARK_MASTER=yarn,spark.executorEnv.SPARK_MASTER=yarn \
	-- pagerank --edges gs://<your-bucket>/data/edges.csv --nodes gs://<your-bucket>/data/nodes.csv --output gs://<your-bucket>/output --iterations 12 --damping 0.85 --has-timestamp --top-n 25
```

Triangle/community:

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.graphanalytics.app.Main \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.yarn.appMasterEnv.SPARK_MASTER=yarn,spark.executorEnv.SPARK_MASTER=yarn \
	-- triangle --edges gs://<your-bucket>/data/edges.csv --output gs://<your-bucket>/output --has-timestamp --top-k 40 --sample-fraction 0.25
```

Temporal PageRank:

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.graphanalytics.app.Main \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.yarn.appMasterEnv.SPARK_MASTER=yarn,spark.executorEnv.SPARK_MASTER=yarn \
	-- temporal --edges gs://<your-bucket>/data/edges.csv --output gs://<your-bucket>/output --iterations 8 --damping 0.85
```

Skew benchmark:

```bash
gcloud dataproc jobs submit spark \
	--cluster=<your-cluster-name> \
	--region=<your-region> \
	--class=com.graphanalytics.app.Main \
	--jars=gs://<your-bucket>/jars/<assembly-jar-name> \
	--properties=spark.yarn.appMasterEnv.SPARK_MASTER=yarn,spark.executorEnv.SPARK_MASTER=yarn \
	-- skew --edges gs://<your-bucket>/data/edges.csv --output gs://<your-bucket>/output --has-timestamp --heavy-keys 10 --salt-buckets 12 --scale-fractions 0.1,0.25,0.5,0.75,1.0
```

### Step 4: Dataproc output location

After successful execution, results are written under:

- gs://<your-bucket>/output/pagerank_top
- gs://<your-bucket>/output/pagerank_convergence
- gs://<your-bucket>/output/pagerank_summary
- gs://<your-bucket>/output/triangle_high_degree_clustering
- gs://<your-bucket>/output/triangle_high_degree_clustering_approx
- gs://<your-bucket>/output/triangle_summary
- gs://<your-bucket>/output/temporal_pagerank_by_day
- gs://<your-bucket>/output/temporal_volatility
- gs://<your-bucket>/output/skew_scaling_summary

---

## 13. Environment Variables Reference

This project can run without environment variables for local execution (because local commands pass all required arguments).

For cluster execution, these are the relevant variables:

- SPARK_MASTER: Spark master URL used by SparkFactory
	- Local example: local[*]
	- Dataproc/YARN example: yarn
- JAVA_HOME: Java runtime path (recommended Java 11)

How to pass SPARK_MASTER in Dataproc:

```bash
--properties=spark.yarn.appMasterEnv.SPARK_MASTER=yarn,spark.executorEnv.SPARK_MASTER=yarn
```

Notes:

- Input/output data paths are provided as CLI arguments (`--edges`, `--nodes`, `--output`) and support both local filesystem and GCS paths.
- If `--nodes` is omitted in PageRank, the node list is derived from edges.

---

## 10. Expected Output Folders

After a successful full run, we can see folders under output/local similar to:

- pagerank_top
- pagerank_convergence
- pagerank_summary
- triangle_high_degree_clustering
- triangle_high_degree_clustering_approx
- triangle_summary
- temporal_pagerank_by_day
- temporal_volatility
- skew_scaling_summary
- dashboard

Each Spark output folder contains part files and _SUCCESS markers.

---

## 11. Troubleshooting

### Issue: Unknown argument pagerank when using sbt run
Cause: sbt run uses default main class and may not target the subcommand CLI.

Fix:
Use runMain with explicit class:

```bash
sbt "runMain com.graphanalytics.app.Main pagerank --edges data/edges.csv --nodes data/nodes.csv --output output/local --iterations 12 --damping 0.85 --has-timestamp --top-n 25"
```

### Issue: Input file not found
Cause: Incorrect path.

Fix:

- Confirm files exist in data/ or data/sample/
- Update --edges and --nodes accordingly

### Issue: Temporal output appears empty
Cause: Missing/invalid timestamp values in edges.csv.

Fix:
Use valid timestamp format: YYYY-MM-DD HH:mm:ss

### Issue: Spark/Java startup errors
Fix:
Use Java 11 and re-run.

