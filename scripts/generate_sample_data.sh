#!/usr/bin/env bash
set -euo pipefail

OUT_DIR=${1:-data/sample}
mkdir -p "$OUT_DIR"

EDGES_FILE="$OUT_DIR/edges.csv"
NODES_FILE="$OUT_DIR/nodes.csv"

echo "src,dst,timestamp" > "$EDGES_FILE"

echo "node_id,type,region" > "$NODES_FILE"
for n in $(seq 1 300); do
  region=$(( (n % 5) + 1 ))
  echo "n${n},user,r${region}" >> "$NODES_FILE"
done

for day in $(seq 1 10); do
  date="2026-01-$(printf "%02d" "$day") 00:00:00"

  # Hub-heavy traffic to create skew.
  for i in $(seq 1 220); do
    dst=$(( (i + day) % 300 + 1 ))
    echo "n1,n${dst},${date}" >> "$EDGES_FILE"
  done

  # General network edges.
  for i in $(seq 1 1200); do
    src=$(( (i * 13 + day) % 300 + 1 ))
    dst=$(( (i * 29 + 2 * day) % 300 + 1 ))
    if [ "$src" -ne "$dst" ]; then
      echo "n${src},n${dst},${date}" >> "$EDGES_FILE"
    fi
  done

done

echo "Generated: $EDGES_FILE"
echo "Generated: $NODES_FILE"
