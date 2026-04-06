#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def find_csv_file(dataset_dir: Path) -> Optional[Path]:
    csv_files = sorted(path for path in dataset_dir.rglob("*.csv") if path.is_file())
    return csv_files[0] if csv_files else None


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def to_float(value: Optional[str], default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Optional[str], default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def humanize_number(value: Any) -> str:
    if isinstance(value, float):
        if value >= 1000 or value.is_integer():
            return f"{value:,.0f}"
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def collect_dataset(input_root: Path, relative_path: str) -> List[Dict[str, str]]:
    dataset_dir = input_root / relative_path
    csv_file = find_csv_file(dataset_dir)
    if csv_file is None:
        return []
    return read_csv_rows(csv_file)


def build_card(title: str, value: Any, subtitle: str = "") -> str:
    subtitle_html = f'<div class="card-subtitle">{html.escape(subtitle)}</div>' if subtitle else ""
    return f'''
      <article class="metric-card">
        <div class="card-title">{html.escape(title)}</div>
        <div class="card-value">{html.escape(humanize_number(value))}</div>
        {subtitle_html}
      </article>
    '''


def top_records(rows: Sequence[Dict[str, str]], key: str, limit: int = 15) -> List[Dict[str, str]]:
    return sorted(rows, key=lambda row: to_float(row.get(key), 0.0), reverse=True)[:limit]


def select_top_temporal_nodes(rows: Sequence[Dict[str, str]], limit: int = 6) -> List[str]:
    buckets: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for row in rows:
        node = row.get("node")
        day = row.get("day")
        rank = to_float(row.get("rank"), 0.0)
        if node and day:
            buckets[node].append((day, rank))

    scored = []
    for node, points in buckets.items():
        if not points:
            continue
        mean_rank = sum(rank for _, rank in points) / len(points)
        scored.append((node, mean_rank))

    scored.sort(key=lambda item: item[1], reverse=True)
    return [node for node, _ in scored[:limit]]


def chart_spec(chart_id: str, title: str, figure: Dict[str, Any], height: int = 420) -> str:
    figure_json = json.dumps(figure, separators=(",", ":"))
    return f'''
      <section class="panel">
        <div class="panel-heading">
          <h2>{html.escape(title)}</h2>
        </div>
        <div id="{chart_id}" class="chart"></div>
        <script>
          window.__dashboardFigures = window.__dashboardFigures || [];
          window.__dashboardFigures.push({{"id": {json.dumps(chart_id)}, "figure": {figure_json}, "height": {height}}});
        </script>
      </section>
    '''


def make_figure(data: List[Dict[str, Any]], layout: Dict[str, Any]) -> Dict[str, Any]:
    return {"data": data, "layout": layout, "config": {"responsive": True, "displayModeBar": False}}


def build_dashboard(input_root: Path) -> Tuple[str, List[str]]:
    cards: List[str] = []
    panels: List[str] = []
    notes: List[str] = []

    pagerank_summary = collect_dataset(input_root, "pagerank_summary")
    pagerank_top = collect_dataset(input_root, "pagerank_top")
    pagerank_convergence = collect_dataset(input_root, "pagerank_convergence")
    triangle_summary = collect_dataset(input_root, "triangle_summary")
    triangle_high_degree = collect_dataset(input_root, "triangle_high_degree_clustering")
    triangle_high_degree_approx = collect_dataset(input_root, "triangle_high_degree_clustering_approx")
    temporal_ranks = collect_dataset(input_root, "temporal_pagerank_by_day")
    temporal_volatility = collect_dataset(input_root, "temporal_volatility")
    skew_summary = collect_dataset(input_root, "skew_scaling_summary")

    if pagerank_summary:
        row = pagerank_summary[0]
        cards.append(build_card("PageRank runtime", to_int(row.get("runtime_ms")), "Spark job wall time in ms"))
        cards.append(build_card("Node count", to_int(row.get("node_count"))))
        cards.append(build_card("Edge count", to_int(row.get("edge_count"))))
        cards.append(build_card("Iterations", to_int(row.get("iterations"))))
    else:
        notes.append("PageRank summary not found.")

    if triangle_summary:
        row = triangle_summary[0]
        cards.append(build_card("Exact triangles", to_int(row.get("exact_triangle_count"))))
        cards.append(build_card("Approx triangles", to_float(row.get("approx_triangle_estimate"), 0.0)))
        cards.append(build_card("Sample fraction", to_float(row.get("edge_sample_fraction"), 0.0)))
    else:
        notes.append("Triangle summary not found.")

    if skew_summary:
        cards.append(build_card("Best skew speedup", max(to_float(r.get("speedup"), 0.0) for r in skew_summary), "Baseline runtime / salted runtime"))
        cards.append(build_card("Largest benchmark", max(to_int(r.get("edge_count")) for r in skew_summary)))
    else:
        notes.append("Skew summary not found.")

    if pagerank_top:
        top_nodes = top_records(pagerank_top, "rank", 15)
        figure = make_figure(
            [{"type": "bar", "x": [to_float(row.get("rank"), 0.0) for row in top_nodes][::-1], "y": [row.get("node", "") for row in top_nodes][::-1], "orientation": "h", "marker": {"color": "#60a5fa"}}],
            {
                "margin": {"l": 110, "r": 20, "t": 10, "b": 50},
                "xaxis": {"title": "Rank", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis": {"title": "Node", "automargin": True},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "Top PageRank nodes", "x": 0.02},
            },
        )
        panels.append(chart_spec("pagerank-top", "Top PageRank nodes", figure))

    if pagerank_convergence:
        iterations = [to_int(row.get("iteration")) for row in pagerank_convergence]
        l1_delta = [to_float(row.get("l1_delta"), 0.0) for row in pagerank_convergence]
        runtime_ms = [to_float(row.get("iteration_runtime_ms"), 0.0) for row in pagerank_convergence]
        figure = make_figure(
            [
                {"type": "scatter", "mode": "lines+markers", "name": "L1 delta", "x": iterations, "y": l1_delta, "line": {"color": "#f97316", "width": 3}},
                {"type": "scatter", "mode": "lines+markers", "name": "Iteration runtime (ms)", "x": iterations, "y": runtime_ms, "line": {"color": "#22c55e", "width": 3}, "yaxis": "y2"},
            ],
            {
                "margin": {"l": 70, "r": 70, "t": 10, "b": 50},
                "xaxis": {"title": "Iteration", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis": {"title": "L1 delta", "gridcolor": "rgba(148,163,184,0.22)", "type": "log"},
                "yaxis2": {"title": "Runtime (ms)", "overlaying": "y", "side": "right", "showgrid": False},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "PageRank convergence", "x": 0.02},
            },
        )
        panels.append(chart_spec("pagerank-convergence", "PageRank convergence", figure))

    if triangle_high_degree:
        nodes = [row.get("node", "") for row in triangle_high_degree[:15]]
        triangle_counts = [to_float(row.get("triangle_count"), 0.0) for row in triangle_high_degree[:15]]
        coeffs = [to_float(row.get("local_clustering_coeff"), 0.0) for row in triangle_high_degree[:15]]
        figure = make_figure(
            [
                {"type": "bar", "name": "Triangle count", "x": nodes, "y": triangle_counts, "marker": {"color": "#38bdf8"}},
                {"type": "scatter", "mode": "lines+markers", "name": "Clustering coeff", "x": nodes, "y": coeffs, "line": {"color": "#facc15", "width": 3}, "yaxis": "y2"},
            ],
            {
                "margin": {"l": 70, "r": 70, "t": 10, "b": 100},
                "xaxis": {"title": "Node", "tickangle": -35},
                "yaxis": {"title": "Triangle count", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis2": {"title": "Local clustering coeff", "overlaying": "y", "side": "right", "showgrid": False},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "Triangle/community signal for high-degree nodes", "x": 0.02},
                "barmode": "group",
            },
        )
        panels.append(chart_spec("triangle-high-degree", "Triangle/community signal", figure, height=460))

    if triangle_high_degree_approx:
        notes.append(f"Approx triangle panel available for {len(triangle_high_degree_approx)} nodes.")

    if temporal_ranks:
        top_nodes = select_top_temporal_nodes(temporal_ranks, 6)
        by_day: Dict[str, Dict[str, float]] = defaultdict(dict)
        day_order = sorted({row.get("day", "") for row in temporal_ranks if row.get("day")})
        for row in temporal_ranks:
            day = row.get("day", "")
            node = row.get("node", "")
            rank = to_float(row.get("rank"), 0.0)
            if day and node:
                by_day[node][day] = rank

        traces = []
        for node in top_nodes:
            traces.append(
                {
                    "type": "scatter",
                    "mode": "lines+markers",
                    "name": node,
                    "x": day_order,
                    "y": [by_day.get(node, {}).get(day, None) for day in day_order],
                    "line": {"width": 3},
                }
            )

        figure = make_figure(
            traces,
            {
                "margin": {"l": 70, "r": 20, "t": 10, "b": 60},
                "xaxis": {"title": "Day", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis": {"title": "Rank", "gridcolor": "rgba(148,163,184,0.22)"},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "Temporal PageRank by day", "x": 0.02},
            },
        )
        panels.append(chart_spec("temporal-pagerank", "Temporal PageRank by day", figure, height=460))

    if temporal_volatility:
        top_vol = sorted(temporal_volatility, key=lambda row: to_float(row.get("rank_volatility"), 0.0), reverse=True)[:15]
        figure = make_figure(
            [
                {"type": "bar", "x": [row.get("node", "") for row in top_vol], "y": [to_float(row.get("rank_volatility"), 0.0) for row in top_vol], "marker": {"color": "#a855f7"}},
            ],
            {
                "margin": {"l": 70, "r": 20, "t": 10, "b": 100},
                "xaxis": {"title": "Node", "tickangle": -35},
                "yaxis": {"title": "Rank volatility", "gridcolor": "rgba(148,163,184,0.22)"},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "Most volatile PageRank nodes", "x": 0.02},
            },
        )
        panels.append(chart_spec("temporal-volatility", "Temporal volatility", figure))

    if skew_summary:
        rows = sorted(skew_summary, key=lambda row: to_float(row.get("scale_fraction"), 0.0))
        figure = make_figure(
            [
                {"type": "scatter", "mode": "lines+markers", "name": "Baseline runtime", "x": [to_float(row.get("scale_fraction"), 0.0) for row in rows], "y": [to_float(row.get("baseline_runtime_ms"), 0.0) for row in rows], "line": {"color": "#38bdf8", "width": 3}},
                {"type": "scatter", "mode": "lines+markers", "name": "Salted runtime", "x": [to_float(row.get("scale_fraction"), 0.0) for row in rows], "y": [to_float(row.get("salted_runtime_ms"), 0.0) for row in rows], "line": {"color": "#f97316", "width": 3}},
                {"type": "scatter", "mode": "lines+markers", "name": "Speedup", "x": [to_float(row.get("scale_fraction"), 0.0) for row in rows], "y": [to_float(row.get("speedup"), 0.0) for row in rows], "line": {"color": "#22c55e", "width": 3}, "yaxis": "y2"},
            ],
            {
                "margin": {"l": 70, "r": 70, "t": 10, "b": 60},
                "xaxis": {"title": "Scale fraction", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis": {"title": "Runtime (ms)", "gridcolor": "rgba(148,163,184,0.22)"},
                "yaxis2": {"title": "Speedup", "overlaying": "y", "side": "right", "showgrid": False},
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(15,23,42,0.35)",
                "font": {"color": "#e2e8f0"},
                "title": {"text": "Skew mitigation scaling", "x": 0.02},
            },
        )
        panels.append(chart_spec("skew-scaling", "Skew mitigation scaling", figure, height=460))

    body_cards = "".join(cards) if cards else '<article class="metric-card empty">No summary metrics found.</article>'
    body_panels = "".join(panels) if panels else '<section class="panel"><div class="panel-heading"><h2>No charts available</h2></div><p>Run the Spark jobs first so the dashboard has CSV inputs.</p></section>'
    footer_notes = "".join(f"<li>{html.escape(note)}</li>" for note in notes)

    template = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Graph Analytics Dashboard</title>
  <script src=\"https://cdn.plot.ly/plotly-2.32.0.min.js\"></script>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #08111f;
      --bg-soft: rgba(15, 23, 42, 0.72);
      --panel: rgba(15, 23, 42, 0.86);
      --border: rgba(148, 163, 184, 0.22);
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #60a5fa;
      --accent-2: #f97316;
      --accent-3: #22c55e;
    }}

    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", system-ui, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(96, 165, 250, 0.24), transparent 28%),
        radial-gradient(circle at top right, rgba(34, 197, 94, 0.12), transparent 30%),
        linear-gradient(180deg, #040814 0%, #08111f 38%, #0b1220 100%);
      min-height: 100vh;
    }}

    .shell {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}

    .hero {{
      display: grid;
      gap: 20px;
      grid-template-columns: 1.6fr 1fr;
      align-items: end;
      margin-bottom: 24px;
    }}

    .hero-copy {{
      padding: 28px;
      border: 1px solid var(--border);
      border-radius: 24px;
      background: linear-gradient(180deg, rgba(15, 23, 42, 0.92), rgba(15, 23, 42, 0.74));
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
      backdrop-filter: blur(10px);
    }}

    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 12px;
      color: var(--accent);
      margin-bottom: 12px;
    }}

    h1 {{
      font-size: clamp(32px, 5vw, 58px);
      line-height: 0.98;
      margin: 0 0 14px;
      max-width: 12ch;
    }}

    .hero-copy p {{
      margin: 0;
      max-width: 70ch;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.65;
    }}

    .hero-aside {{
      padding: 22px;
      border-radius: 24px;
      border: 1px solid var(--border);
      background: rgba(7, 11, 21, 0.78);
      color: var(--muted);
    }}

    .hero-aside strong {{ color: var(--text); }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 24px;
    }}

    .metric-card {{
      padding: 18px 18px 16px;
      border: 1px solid var(--border);
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(15, 23, 42, 0.92), rgba(15, 23, 42, 0.68));
      box-shadow: 0 18px 50px rgba(0, 0, 0, 0.22);
      min-height: 118px;
    }}

    .metric-card.empty {{ display: grid; place-items: center; color: var(--muted); }}
    .card-title {{ color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .card-value {{ font-size: 30px; font-weight: 700; margin-top: 10px; }}
    .card-subtitle {{ margin-top: 6px; color: var(--muted); font-size: 12px; }}

    .panel-stack {{ display: grid; gap: 18px; }}
    .panel {{
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 24px;
      background: linear-gradient(180deg, rgba(15, 23, 42, 0.88), rgba(15, 23, 42, 0.66));
      box-shadow: 0 18px 50px rgba(0, 0, 0, 0.22);
    }}

    .panel-heading {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}

    .panel-heading h2 {{
      margin: 0;
      font-size: 20px;
      letter-spacing: -0.02em;
    }}

    .chart {{ width: 100%; min-height: 420px; }}
    ul.notes {{
      margin: 14px 0 0;
      padding-left: 20px;
      color: var(--muted);
      line-height: 1.7;
    }}

    @media (max-width: 1120px) {{
      .hero {{ grid-template-columns: 1fr; }}
      .metric-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}

    @media (max-width: 720px) {{
      .shell {{ padding: 18px 14px 28px; }}
      .metric-grid {{ grid-template-columns: 1fr; }}
      .hero-copy, .hero-aside, .panel {{ border-radius: 18px; }}
    }}
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"hero\">
      <div class=\"hero-copy\">
        <div class=\"eyebrow\">Dataproc metrics dashboard</div>
        <h1>Graph analytics you can inspect in the browser.</h1>
        <p>This dashboard turns the Spark job outputs into interactive charts so the Dataproc results are easy to review in GCP. Upload the generated <strong>index.html</strong> to Cloud Storage or open it locally after the job run.</p>
      </div>
      <div class=\"hero-aside\">
        <strong>What this dashboard covers</strong>
        <p style=\"margin: 10px 0 0;\">PageRank top nodes and convergence, triangle/community structure, temporal volatility, and skew mitigation scaling are all rendered from the CSV outputs under the job output directory.</p>
      </div>
    </div>

    <div class=\"metric-grid\">{body_cards}</div>

    <div class=\"panel-stack\">{body_panels}</div>

    <ul class=\"notes\">{footer_notes}</ul>
  </div>

  <script>
    const charts = window.__dashboardFigures || [];
    charts.forEach(({{ id, figure, height }}) => {{
      const target = document.getElementById(id);
      if (!target) return;
      target.style.minHeight = `${{height}}px`;
      Plotly.newPlot(target, figure.data, figure.layout, figure.config);
    }});
  </script>
</body>
</html>
"""

    return template, notes


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate an interactive HTML dashboard from Spark graph analytics outputs.")
    parser.add_argument("--input", required=True, help="Path to the directory that contains the job output folders.")
    parser.add_argument("--output", required=True, help="Directory where the dashboard files should be written.")
    args = parser.parse_args()

    input_root = Path(args.input).expanduser().resolve()
    output_root = Path(args.output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    html_text, notes = build_dashboard(input_root)
    dashboard_path = output_root / "index.html"
    dashboard_path.write_text(html_text, encoding="utf-8")

    summary_path = output_root / "dashboard-summary.json"
    summary_path.write_text(json.dumps({"input": str(input_root), "output": str(output_root), "notes": notes}, indent=2), encoding="utf-8")

    print(f"Dashboard written to: {dashboard_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())