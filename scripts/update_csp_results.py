#!/usr/bin/env python3
"""Fetch selected CSP800 logs and generate docs/csp.html."""

from __future__ import annotations

import csv
import argparse
import html
import json
import re
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

csv.field_size_limit(sys.maxsize)

REMOTE = "b39275@laurel.kudpc.kyoto-u.ac.jp"
GR10672 = "/LARGE0/gr10672/b39275/xcsp3instances"
GR10609 = "/LARGE0/gr10609/b39275/xcsp3instances"
HOME_ROOT = "/home/b/b39275/xcsp3instances"
CSP_LIST_REMOTE = f"{GR10609}/adhoc/c22-c25-csp800/competition_all_c22_c25_csp_only.csv"
RAW_LOG_BASE = "https://raw.githubusercontent.com/TakehideSoh/improving-kat/main/"
SOLUTION_VALIDATION_CSV = "csp800_solution_checker_20260517.csv"
SAT_UNSAT_CONSISTENCY_LABELS = [
    "ae651e02 direct-order 2opt-off",
    "856af1fd dirty log-scop autoBDD agg",
    "261d4b72 dirty portfolio v2 autoBDD agg",
    "0a36be0b portfolio v2 shortsum2m autoBDD agg",
    "ACE 64G rr",
]


@dataclass(frozen=True)
class Run:
    slug: str
    column: str
    result_dir: str
    runsolver_prefix: str
    remote_base: str
    options: str = ""


RUNS = [
    Run(
        "csp800-b9f938ec-direct-order",
        "b9f938ec direct-order",
        "kat-c22-c25-csp800-order-direct-mdd-tl-20260429-b9f938ec-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-20260429-b9f938ec-q10672",
        GR10672,
    ),
    Run(
        "csp800-b9f938ec-log-scop",
        "b9f938ec log-scop",
        "kat-c22-c25-csp800-log-scop-mdd-tl-20260427-b9f938ec-q10672",
        "runsolver-kat-c22-c25-csp800-log-scop-mdd-tl-20260427-b9f938ec-q10672",
        GR10672,
    ),
    Run(
        "csp800-0a48c5bb-portfolio-stage1",
        "0a48c5bb portfolio stage1",
        "kat-c22-c25-csp800-portfolio-mdd-tl-no-budget-alldiff-stage1-20260419-0a48c5bb",
        "runsolver-kat-c22-c25-csp800-portfolio-mdd-tl-no-budget-alldiff-stage1-20260419-0a48c5bb",
        GR10672,
    ),
    Run(
        "pycsp3-extra-ortools-csp800-1t-20260507",
        "pycsp3-extra OR-Tools 1t 20260507",
        "pycsp3-extra-ortools-csp800-1t-20260507-q10672",
        "runsolver-pycsp3-extra-ortools-csp800-1t-20260507-q10672",
        GR10672,
    ),
    Run(
        "csp800-835f8aaf-direct-order",
        "835f8aaf direct-order",
        "kat-c22-c25-csp800-order-direct-mdd-tl-scip-dynamic-20260508-835f8aaf-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-scip-dynamic-20260508-835f8aaf-q10672",
        GR10672,
    ),
    Run(
        "csp800-835f8aaf-log-scop",
        "835f8aaf log-scop",
        "kat-c22-c25-csp800-log-scop-mdd-tl-scip-dynamic-20260508-835f8aaf-q10672",
        "runsolver-kat-c22-c25-csp800-log-scop-mdd-tl-scip-dynamic-20260508-835f8aaf-q10672",
        GR10672,
        "--encoder log-scop --order-ge-table mdd-tl --progress",
    ),
    Run(
        "csp800-1173b3f4-order-direct-extprop8196-s40",
        "1173b3f4 order-direct extprop8196 s40",
        "kat-c22-c25-csp800-order-direct-mdd-tl-extprop-dp8196-s40-20260511-1173b3f4-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-extprop-dp8196-s40-20260511-1173b3f4-q10672",
        GR10672,
    ),
    Run(
        "csp800-10c9c43b-order-direct-extprop-rule-dp1e10-specs200-saved1m",
        "10c9c43b order-direct extprop rule dp1e10 specs200 saved1m",
        "kat-c22-c25-csp800-order-direct-mdd-tl-extprop-rule-dp1e10-specs200-saved1m-20260513-10c9c43b-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-extprop-rule-dp1e10-specs200-saved1m-20260513-10c9c43b-q10672",
        GR10672,
    ),
    Run(
        "csp800-10c9c43b-order-direct-extprop-rule-dp1e10-specs200-saved1m-eqne2",
        "10c9c43b order-direct extprop rule dp1e10 specs200 saved1m eqne2",
        "kat-c22-c25-csp800-order-direct-mdd-tl-extprop-rule-dp1e10-specs200-saved1m-eqne2-20260514-10c9c43b-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-extprop-rule-dp1e10-specs200-saved1m-eqne2-20260514-10c9c43b-q10672",
        GR10672,
    ),
    Run(
        "csp800-ae651e02-direct-order",
        "ae651e02 direct-order 2opt-off",
        "kat-c22-c25-csp800-order-direct-mdd-tl-20260515-ae651e02-q10672",
        "runsolver-kat-c22-c25-csp800-order-direct-mdd-tl-20260515-ae651e02-q10672",
        GR10672,
    ),
    Run(
        "csp800-ee715ee4-log-scop-autobdd",
        "ee715ee4 log-scop autoBDD",
        "kat-c22-c25-csp800-log-scop-mdd-tl-autobdd-20260515-ee715ee4-q10672",
        "runsolver-kat-c22-c25-csp800-log-scop-mdd-tl-autobdd-20260515-ee715ee4-q10672",
        GR10672,
        "--encoder log-scop --order-ge-table mdd-tl --progress (default: auto-monotone-bdd2)",
    ),
    Run(
        "csp800-856af1fd-dirty-log-scop-autobdd-agg",
        "856af1fd dirty log-scop autoBDD agg",
        "kat-c22-c25-csp800-log-scop-mdd-tl-autobdd-agg-20260515-856af1fd-dirty-q10609",
        "runsolver-kat-c22-c25-csp800-log-scop-mdd-tl-autobdd-agg-20260515-856af1fd-dirty-q10609",
        GR10609,
        "--encoder log-scop --order-ge-table mdd-tl --log-scop-linear-pb-backend auto --log-scop-bdd-decomposition auto --log-scop-aggregate-weighted-lits --progress",
    ),
    Run(
        "csp800-261d4b72-portfolio-autobdd-agg",
        "[BUG] 261d4b72 portfolio autoBDD agg",
        "kat-c22-c25-csp800-portfolio-mdd-tl-autobdd-agg-20260516-261d4b72-q10672",
        "runsolver-kat-c22-c25-csp800-portfolio-mdd-tl-autobdd-agg-20260516-261d4b72-q10672",
        GR10672,
        "--encoder portfolio --order-ge-table mdd-tl --log-scop-linear-pb-backend auto --log-scop-bdd-decomposition auto --log-scop-aggregate-weighted-lits --progress",
    ),
    Run(
        "csp800-261d4b72-dirty-portfolio-v2-autobdd-agg",
        "261d4b72 dirty portfolio v2 autoBDD agg",
        "kat-c22-c25-csp800-portfolio-v2-mdd-tl-autobdd-agg-20260517-261d4b72-dirty-q10672",
        "runsolver-kat-c22-c25-csp800-portfolio-v2-mdd-tl-autobdd-agg-20260517-261d4b72-dirty-q10672",
        GR10672,
        "--encoder portfolio --portfolio-strategy v2 --order-ge-table mdd-tl --log-scop-linear-pb-backend auto --log-scop-bdd-decomposition auto --log-scop-aggregate-weighted-lits --progress",
    ),
    Run(
        "csp800-0a36be0b-portfolio-v2-shortsum2m-autobdd-agg",
        "0a36be0b portfolio v2 shortsum2m autoBDD agg",
        "kat-c22-c25-csp800-portfolio-v2-shortsum2m-mdd-tl-autobdd-agg-20260517-0a36be0b-q10672",
        "runsolver-kat-c22-c25-csp800-portfolio-v2-shortsum2m-mdd-tl-autobdd-agg-20260517-0a36be0b-q10672",
        GR10672,
        "--encoder portfolio --portfolio-strategy v2 --order-ge-table mdd-tl --log-scop-linear-pb-backend auto --log-scop-bdd-decomposition auto --log-scop-aggregate-weighted-lits --progress",
    ),
]


CSS = """:root { color-scheme: light; }
body { margin: 0; padding: 2rem; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.55; color: #1f2937; background: #f8fafc; }
main { max-width: 1440px; margin: 0 auto; background: white; padding: 2rem; border: 1px solid #e5e7eb; border-radius: 12px; box-shadow: 0 1px 3px rgb(15 23 42 / 0.08); }
h1, h2 { border-bottom: 1px solid #e5e7eb; padding-bottom: .25rem; }
a { color: #2563eb; text-decoration: none; }
a:hover { text-decoration: underline; }
code { background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 4px; padding: 0 .25em; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .92em; }
.table-wrap { overflow: auto; max-height: 82vh; margin: 1rem 0 2rem; border: 1px solid #d1d5db; }
table { border-collapse: separate; border-spacing: 0; min-width: max-content; font-size: .9rem; }
th, td { border-right: 1px solid #d1d5db; border-bottom: 1px solid #d1d5db; padding: .35rem .5rem; vertical-align: top; background: white; }
th { background: #f1f5f9; }
tr:nth-child(even) td { background: #fafafa; }
.sticky-col-label { position: sticky; top: 0; z-index: 5; box-shadow: 0 1px 0 #d1d5db; }
.sticky-row-label { position: sticky; z-index: 4; box-shadow: 1px 0 0 #d1d5db; }
.sticky-row-label-1 { left: 0; }
.sticky-corner-label { z-index: 7; }
"""


SCRIPT = """(() => {
  function setupStickyLabels() {
    document.querySelectorAll('.table-wrap table').forEach((table) => {
      const headerCells = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
      headerCells.forEach((cell) => cell.classList.add('sticky-col-label'));
      const firstColumnWidth = Math.ceil(headerCells[0]?.getBoundingClientRect().width || 0);
      Array.from(table.rows).forEach((row) => {
        const first = row.cells[0];
        const second = row.cells[1];
        if (first) {
          first.classList.add('sticky-row-label', 'sticky-row-label-1');
          first.style.left = '0px';
        }
        if (second) {
          second.classList.add('sticky-row-label', 'sticky-row-label-2');
          second.style.left = `${firstColumnWidth}px`;
        }
      });
      if (headerCells[0]) headerCells[0].classList.add('sticky-corner-label');
      if (headerCells[1]) headerCells[1].classList.add('sticky-corner-label');
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setupStickyLabels);
  } else {
    setupStickyLabels();
  }
  window.addEventListener('resize', setupStickyLabels);
})();"""


def root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def logs_root(root: Path) -> Path:
    return root / "docs" / "logs" / "csp"


def run_cmd(args: list[str], *, input_text: str | None = None) -> str:
    print("+", " ".join(args), file=sys.stderr)
    proc = subprocess.run(
        args,
        check=True,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
    )
    return proc.stdout


def rel_link(root: Path, path: Path | None, label: str) -> str:
    if path is None:
        return html.escape(label)
    rel = path.relative_to(root).as_posix()
    url = RAW_LOG_BASE + rel
    return f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>'


def fetch_instance_list(root: Path) -> None:
    dest = logs_root(root) / "competition_all_c22_c25_csp_only.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(["rsync", "-az", f"{REMOTE}:{CSP_LIST_REMOTE}", str(dest)])


def read_instances(root: Path) -> list[tuple[int, str]]:
    path = logs_root(root) / "competition_all_c22_c25_csp_only.csv"
    rows: list[tuple[int, str]] = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            rows.append((int(row["instance_id"]), row["instance_relpath"]))
    return rows


def fetch_kat_run(root: Path, run: Run) -> None:
    local = logs_root(root) / run.slug
    (local / "rows").mkdir(parents=True, exist_ok=True)
    (local / "runsolver").mkdir(parents=True, exist_ok=True)
    run_cmd(
        [
            "rsync",
            "-az",
            f"{REMOTE}:{run.remote_base}/results/{run.result_dir}/rows/",
            str(local / "rows") + "/",
        ]
    )
    run_cmd(
        [
            "rsync",
            "-az",
            "--prune-empty-dirs",
            f"--include={run.runsolver_prefix}-[0-9]*/",
            f"--include={run.runsolver_prefix}-[0-9]*/output.log",
            f"--include={run.runsolver_prefix}-[0-9]*/values.log",
            "--exclude=*",
            f"{REMOTE}:{run.remote_base}/slurm-logs/runsolver/",
            str(local / "runsolver") + "/",
        ]
    )


def fetch_ace_logs(root: Path) -> None:
    instances_json = json.dumps(read_instances(root))
    manifest_script = r"""
import json
from pathlib import Path
import re

instances = json.loads(INSTANCES_JSON)
base = Path('/home/b/b39275/xcsp3instances/slurm-logs/runsolver')
logs_by_name = {}
for out in sorted(base.glob('runsolver-ace64g-rr-*.out')):
    try:
        text = out.read_text(errors='replace')
    except OSError:
        continue
    m = re.search(r'^\s*name:([^\n]+)', text, re.MULTILINE)
    if not m:
        continue
    logs_by_name[m.group(1).strip()] = out

records = []
files = []
for instance_id, relpath in instances:
    suffix = Path(relpath).name
    stem = suffix[:-9] if suffix.endswith('.xml.lzma') else suffix
    out = logs_by_name.get(stem)
    if out:
        files.append(out.name)
        var = out.with_suffix('.var')
        if var.exists():
            files.append(var.name)
        records.append({'instance_id': instance_id, 'relpath': relpath, 'out': out.name, 'var': var.name if var.exists() else None})
print(json.dumps({'files': files, 'records': records}))
"""
    script = "INSTANCES_JSON = " + repr(instances_json) + "\n" + manifest_script
    manifest = json.loads(run_cmd(["ssh", REMOTE, "python3 -"], input_text=script))
    files = manifest["files"]
    local = logs_root(root) / "csp800-ace-rr" / "runsolver"
    local.mkdir(parents=True, exist_ok=True)
    (logs_root(root) / "csp800-ace-rr" / "manifest.json").write_text(json.dumps(manifest["records"], indent=2))
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        for name in files:
            f.write(name + "\n")
        list_path = Path(f.name)
    try:
        run_cmd(
            [
                "rsync",
                "-az",
                "--files-from",
                str(list_path),
                f"{REMOTE}:{HOME_ROOT}/slurm-logs/runsolver/",
                str(local) + "/",
            ]
        )
    finally:
        list_path.unlink(missing_ok=True)


def row_id(path: Path) -> int | None:
    m = re.match(r"row-(\d+)-(\d+)-(\d+)\.csv$", path.name)
    return int(m.group(3)) if m else None


def load_rows(root: Path, run: Run) -> dict[int, tuple[Path, list[str]]]:
    rows: dict[int, tuple[Path, list[str]]] = {}
    for path in sorted((logs_root(root) / run.slug / "rows").glob("*.csv")):
        instance_id = row_id(path)
        if instance_id is None:
            continue
        text = path.read_text(errors="replace").strip()
        rows[instance_id] = (path, next(csv.reader([text])) if text else [])
    return rows


def kat_log_path(root: Path, run: Run, instance_id: int, row_path: Path | None) -> Path | None:
    base = logs_root(root) / run.slug / "runsolver"
    if row_path is not None:
        m = re.match(r"row-(\d+)-(\d+)-(\d+)\.csv$", row_path.name)
        if m:
            path = base / f"{run.runsolver_prefix}-{m.group(1)}-{m.group(2)}-{m.group(3)}" / "output.log"
            if path.exists():
                return path
    matches = sorted(base.glob(f"{run.runsolver_prefix}-*-*-{instance_id}/output.log"))
    return matches[-1] if matches else None


def values_path(log_path: Path | None) -> Path | None:
    if log_path is None:
        return None
    if log_path.name == "output.log":
        candidate = log_path.with_name("values.log")
    else:
        candidate = log_path.with_suffix(".var")
    return candidate if candidate.exists() else None


def log_outcome(log_path: Path | None) -> str | None:
    if log_path is None:
        return None
    text = log_path.read_text(errors="replace").lower()
    outcome = None
    vals = values_path(log_path)
    if vals is not None:
        vtext = vals.read_text(errors="replace").lower()
        if "memout=true" in vtext:
            outcome = "MO"
        if "timeout=true" in vtext:
            outcome = outcome or "TO"
    if any(token in text for token in ["out_of_memory", "oom", "killed", "exit code 137", "exit_code_137"]):
        outcome = "MO"
    if any(token in text for token in ["timeout", "time limit", "signal=15", "sigterm", "maximum cpu time"]):
        outcome = outcome or "TO"
    return outcome


def classify_kat(fields: list[str], log_path: Path | None) -> str:
    status = fields[1] if len(fields) > 1 else ""
    if status == "sat":
        return "SAT"
    if status == "unsat":
        return "UNSAT"
    if status == "optimum":
        return "OPT"
    if status == "timeout":
        return "TO"
    if status == "memout":
        return "MO"
    outcome = log_outcome(log_path)
    if outcome:
        return outcome
    if status in {"internal_error", "parse_error"}:
        return "ERR"
    return status.upper() if status else "NA"


def load_ace_manifest(root: Path) -> dict[int, str]:
    path = logs_root(root) / "csp800-ace-rr" / "manifest.json"
    if not path.exists():
        return {}
    return {int(row["instance_id"]): row["out"] for row in json.loads(path.read_text())}


def ace_log_path(root: Path, ace_manifest: dict[int, str], instance_id: int) -> Path | None:
    base = logs_root(root) / "csp800-ace-rr" / "runsolver"
    name = ace_manifest.get(instance_id)
    if not name:
        return None
    path = base / name
    return path if path.exists() else None


def classify_ace(log_path: Path | None) -> str:
    if log_path is None:
        return "NA"
    text = log_path.read_text(errors="replace")
    if re.search(r"^s\s+SATISFIABLE", text, re.MULTILINE):
        return "SAT"
    if re.search(r"^s\s+UNSATISFIABLE", text, re.MULTILINE):
        return "UNSAT"
    outcome = log_outcome(log_path)
    return outcome or "UNKNOWN"


def build_summary(labels: list[str], table: list[list[tuple[str, Path | None]]]) -> str:
    counts = {label: Counter(row[i][0] for row in table) for i, label in enumerate(labels)}
    lines = [
        "<h2 id=\"summary\">Summary</h2>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>run</th><th>SAT</th><th>UNSAT</th><th>solved</th><th>TO</th><th>MO</th><th>ERR</th><th>UNKNOWN/NA</th></tr></thead>",
        "<tbody>",
    ]
    for label in labels:
        c = counts[label]
        unknown = sum(v for k, v in c.items() if k not in {"SAT", "UNSAT", "TO", "MO", "ERR"})
        lines.append(
            "<tr>"
            f"<td>{html.escape(label)}</td>"
            f"<td>{c['SAT']}</td><td>{c['UNSAT']}</td><td>{c['SAT'] + c['UNSAT']}</td>"
            f"<td>{c['TO']}</td><td>{c['MO']}</td><td>{c['ERR']}</td><td>{unknown}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table></div>"])
    return "\n".join(lines)


def build_run_options() -> str:
    runs = [run for run in RUNS if run.options]
    if not runs:
        return ""
    lines = [
        "<h2 id=\"run-options\">Run options</h2>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>run</th><th>options</th></tr></thead>",
        "<tbody>",
    ]
    for run in runs:
        lines.append(
            "<tr>"
            f"<td>{html.escape(run.column)}</td>"
            f"<td><code>{html.escape(run.options)}</code></td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table></div>"])
    return "\n".join(lines)


def load_solution_validation(root: Path) -> list[dict[str, str]]:
    path = logs_root(root) / "validation" / SOLUTION_VALIDATION_CSV
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_solution_validation(root: Path) -> str:
    rows = load_solution_validation(root)
    if not rows:
        return ""
    by_label: dict[str, Counter[str]] = {}
    for row in rows:
        label = row.get("label") or row.get("run") or "unknown"
        by_label.setdefault(label, Counter())[row.get("validation", "")] += 1

    validation_path = logs_root(root) / "validation" / SOLUTION_VALIDATION_CSV
    lines = [
        "<h2 id=\"solution-validation\">Solution validation</h2>",
        "<p>SAT と報告された解を <code>xcsp3-solutionChecker-2.6.0.jar</code> で検証した結果。</p>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>run</th><th>checked SAT solutions</th><th>valid</th><th>invalid</th><th>checker_error</th><th>no_solution</th><th>missing_instance</th><th>checker_timeout</th></tr></thead>",
        "<tbody>",
    ]
    for label in sorted(by_label):
        counts = by_label[label]
        checked = sum(counts.values())
        lines.append(
            "<tr>"
            f"<td>{html.escape(label)}</td>"
            f"<td>{checked}</td>"
            f"<td>{counts['valid']}</td>"
            f"<td>{counts['invalid']}</td>"
            f"<td>{counts['checker_error']}</td>"
            f"<td>{counts['no_solution']}</td>"
            f"<td>{counts['missing_instance']}</td>"
            f"<td>{counts['checker_timeout']}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table></div>"])
    lines.append(f"<p>詳細 CSV: {rel_link(root, validation_path, SOLUTION_VALIDATION_CSV)}</p>")
    return "\n".join(lines)


def build_sat_unsat_consistency(
    root: Path,
    instances: list[tuple[int, str]],
    labels: list[str],
    cell_table: list[list[tuple[str, Path | None]]],
) -> str:
    selected = [(label, labels.index(label)) for label in SAT_UNSAT_CONSISTENCY_LABELS if label in labels]
    if len(selected) < 2:
        return ""

    issue_rows: list[str] = []
    pair_counts: Counter[str] = Counter()
    for (instance_id, relpath), cells in zip(instances, cell_table):
        selected_cells = [(label, cells[index]) for label, index in selected]
        statuses = {label: cell[0] for label, cell in selected_cells}
        sat_labels = [label for label, status in statuses.items() if status == "SAT"]
        unsat_labels = [label for label, status in statuses.items() if status == "UNSAT"]
        if not sat_labels or not unsat_labels:
            continue
        for sat_label in sat_labels:
            for unsat_label in unsat_labels:
                pair_counts[f"{sat_label} SAT vs {unsat_label} UNSAT"] += 1
        issue_rows.append(
            "<tr>"
            f"<td>{instance_id}</td>"
            f"<td><code>{html.escape(Path(relpath).name)}</code></td>"
            + "".join(f"<td>{rel_link(root, path, label)}</td>" for _, (label, path) in selected_cells)
            + "</tr>"
        )

    lines = [
        "<h2 id=\"sat-unsat-consistency\">SAT/UNSAT consistency</h2>",
        "<p><code>ae651e02 direct-order 2opt-off</code>、<code>856af1fd dirty log-scop autoBDD agg</code>、<code>261d4b72 dirty portfolio v2 autoBDD agg</code>、<code>0a36be0b portfolio v2 shortsum2m autoBDD agg</code>、<code>ACE 64G rr</code> の間で、同一インスタンスに SAT と UNSAT が混在する矛盾を調べた結果。</p>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>check</th><th>count</th></tr></thead>",
        "<tbody>",
        f"<tr><td>SAT/UNSAT conflicts</td><td>{len(issue_rows)}</td></tr>",
    ]
    for label, count in sorted(pair_counts.items()):
        lines.append(f"<tr><td>{html.escape(label)}</td><td>{count}</td></tr>")
    lines.extend(["</tbody>", "</table></div>"])

    if issue_rows:
        lines.extend(
            [
                "<h3>Conflict instances</h3>",
                "<div class=\"table-wrap\"><table>",
                "<thead><tr><th>#</th><th>instance</th>"
                + "".join(f"<th>{html.escape(label)}</th>" for label, _ in selected)
                + "</tr></thead>",
                "<tbody>",
                *issue_rows,
                "</tbody>",
                "</table></div>",
            ]
        )
    return "\n".join(lines)


def generate_html(root: Path) -> None:
    instances = read_instances(root)
    kat_rows = {run.slug: load_rows(root, run) for run in RUNS}
    ace_manifest = load_ace_manifest(root)
    labels = [run.column for run in RUNS] + ["ACE 64G rr"]
    cell_table: list[list[tuple[str, Path | None]]] = []
    row_lines: list[str] = []
    for instance_id, relpath in instances:
        cells: list[tuple[str, Path | None]] = []
        for run in RUNS:
            row_path, fields = kat_rows[run.slug].get(instance_id, (None, []))
            log_path = kat_log_path(root, run, instance_id, row_path)
            cells.append((classify_kat(fields, log_path), log_path))
        ace_path = ace_log_path(root, ace_manifest, instance_id)
        cells.append((classify_ace(ace_path), ace_path))
        cell_table.append(cells)
        row_lines.append(
            "<tr>"
            f"<td>{instance_id}</td><td><code>{html.escape(Path(relpath).name)}</code></td>"
            + "".join(f"<td>{rel_link(root, path, label)}</td>" for label, path in cells)
            + "</tr>"
        )

    body = [
        "<h1 id=\"csp-results\">CSP Results</h1>",
        f"<p>最終更新: {date.today().isoformat()}</p>",
        "<p>対象は c22-c25 CSP-only 800 instances。複数コミット・設定の kat 結果（従来の direct-order / log-scop / portfolio、835f8aaf SCIP dynamic、1173b3f4 extprop8196 s40、10c9c43b extprop rule / eqne2、ae651e02 direct-order 2opt-off、ee715ee4 log-scop autoBDD、261d4b72 portfolio v2 autoBDD agg、0a36be0b portfolio v2 shortsum2m autoBDD agg など）と OR-Tools、ACE を同期したもの。</p>",
        build_summary(labels, cell_table),
        build_run_options(),
        build_solution_validation(root),
        build_sat_unsat_consistency(root, instances, labels, cell_table),
        "<h2 id=\"csp800-instance-table\">CSP800 instance table</h2>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>#</th><th>instance</th>" + "".join(f"<th>{html.escape(label)}</th>" for label in labels) + "</tr></thead>",
        "<tbody>",
        *row_lines,
        "</tbody>",
        "</table></div>",
    ]
    html_text = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSP Results</title>
<style>
{CSS}
</style>
</head>
<body>
<main>
{chr(10).join(body)}
</main>
<script>
{SCRIPT}
</script>
</body>
</html>
"""
    out = root / "docs" / "csp.html"
    out.write_text(html_text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-fetch", action="store_true", help="Regenerate docs/csp.html from existing logs only")
    args = parser.parse_args()
    root = root_dir()
    if not args.no_fetch:
        fetch_instance_list(root)
        for run in RUNS:
            fetch_kat_run(root, run)
        fetch_ace_logs(root)
    generate_html(root)


if __name__ == "__main__":
    main()
