#!/usr/bin/env bash
set -euo pipefail

EDGES=${1:-data/sample/edges.csv}
NODES=${2:-data/sample/nodes.csv}
OUT=${3:-output/local}

mkdir -p "$OUT"

sbt "run pagerank --edges $EDGES --nodes $NODES --output $OUT --iterations 12 --damping 0.85 --has-timestamp --top-n 25"
sbt "run triangle --edges $EDGES --output $OUT --has-timestamp --top-k 40 --sample-fraction 0.25"
sbt "run temporal --edges $EDGES --output $OUT --iterations 8 --damping 0.85"
sbt "run skew --edges $EDGES --output $OUT --has-timestamp --heavy-keys 10 --salt-buckets 12 --scale-fractions 0.1,0.25,0.5,0.75,1.0"

echo "Outputs written under: $OUT"
