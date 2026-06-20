#!/usr/bin/env python3
"""Fetch MiniZinc Challenge logs and generate docs/minizinc.html."""

from __future__ import annotations

import argparse
import csv
import html
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

csv.field_size_limit(sys.maxsize)

REMOTE = "b39275@laurel.kudpc.kyoto-u.ac.jp"
GR10609 = "/LARGE0/gr10609/b39275/xcsp3instances"
RAW_LOG_BASE = "https://raw.githubusercontent.com/TakehideSoh/improving-kat/main/"
INSTANCE_CSV_REMOTE = (
    f"{GR10609}/instance-lists/"
    "minizinc-challenge-instances-official-develop-recursive-20260620.csv"
)

KAT_ROW_SCHEMA = [
    "instance",
    "status",
    "source_constraints",
    "normalized_constraints",
    "lowered_constraints",
    "unsupported_constraints",
    "sat_num_vars",
    "sat_num_clauses_final",
    "total_ms",
    "propagate_ms",
    "normalize_ms",
    "lower_ms",
    "preencode_ms",
    "encode_ms",
    "sat_solve_ms",
    "root_lp_entered",
    "root_lp_status",
    "objective_naive_lower",
    "objective_naive_upper",
    "objective_lp_lower",
    "objective_lp_upper",
    "root_lp_improved",
    "root_lp_ms",
    "root_lp_rows",
    "root_lp_vars",
    "root_lp_structured_objective_terms",
    "root_lp_structured_objective_aux_vars",
    "root_lp_structured_objective_rows",
    "root_lp_objective_value",
    "reason_code",
    "error",
]

ORTOOLS_ROW_SCHEMA = [
    "instance_id",
    "instance_relpath",
    "data_relpaths",
    "status",
    "wctime",
    "cputime",
    "maxrss",
    "objective",
    "exitstatus",
    "timeout",
    "memout",
    "reason_code",
    "error",
]


@dataclass(frozen=True)
class Run:
    slug: str
    column: str
    result_dir: str
    runsolver_prefix: str
    remote_base: str
    solver: str
    options: str
    time_rsc: str


RUNS = [
    Run(
        "kat-f2b89728-portfolio-v3-1200s-gr10609",
        "KAT f2b89728 dirty portfolio-v3 1200s",
        "kat-mznchallenge-all-1200s-20260620-f2b89728-dirty-mznfull10609-full1801",
        "runsolver-kat-mznchallenge-all-1200s-20260620-f2b89728-dirty-mznfull10609-full1801",
        GR10609,
        "kat",
        "--encoder portfolio --portfolio-strategy v3 --cop-pipeline guarded-basic "
        "--cop-objective-cut direct-expr --cop-objective-cut-timing incremental-soft "
        "--cop-objective-cut-backend cnf --table-encoding mdd-tl --cumulative-mode exact "
        "--cumulative-decomposition time-rd --progress --log-scop-linear-pb-backend auto "
        "--log-scop-bdd-decomposition auto --log-scop-aggregate-weighted-lits "
        "--order-ge-shorten-link-cost --direct-eq-ne-max-arity 2 --eq-ne-encoding direct "
        "--portfolio-log-scop-eq-ne-encoding ge-rewrite --cop-objective-phase auto "
        "--cop-objective-search trisection --cop-objective-search-fallback-budget 180 "
        "--root-lp-bound --root-lp-backend lpo --root-lp-time-limit-secs 60 "
        "--ignore-parse-error",
        "1200s, p=1:t=14:c=14:m=16000M, gr10609b",
    ),
    Run(
        "ortools-b21a132690-1200s-gr10497",
        "OR-Tools b21a132690 dirty 1200s",
        "ortools-mznchallenge-all-1200s-20260620-b21a132690-dirty-q10497-full1801",
        "runsolver-ortools-mznchallenge-all-1200s-20260620-b21a132690-dirty-q10497-full1801",
        GR10609,
        "ortools",
        "solver -v -p 14 --time-limit 1200000",
        "1200s, p=1:t=14:c=14:m=16000M, gr10497b",
    ),
]


def root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def logs_root(root: Path) -> Path:
    return root / "docs" / "logs" / "minizinc"


def run_cmd(args: list[str]) -> None:
    print("+", " ".join(args))
    subprocess.run(args, check=True)


def fetch_instance_list(root: Path) -> None:
    dest = logs_root(root) / "instances.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(["rsync", "-az", f"{REMOTE}:{INSTANCE_CSV_REMOTE}", str(dest)])


def fetch_run(root: Path, run: Run) -> None:
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
    list_path = local / "runsolver-files.txt"
    list_code = """
from pathlib import Path
import sys

base = Path(sys.argv[1])
prefix = sys.argv[2]
for d in sorted(base.glob(prefix + "-*")):
    if not d.is_dir():
        continue
    for name in ("output.log", "values.log"):
        if (d / name).exists():
            print(f"{d.name}/{name}")
"""
    with list_path.open("w") as handle:
        subprocess.run(
            [
                "ssh",
                REMOTE,
                "python3",
                "-",
                f"{run.remote_base}/slurm-logs/runsolver",
                run.runsolver_prefix,
            ],
            input=list_code,
            text=True,
            stdout=handle,
            check=True,
        )
    run_cmd(
        [
            "rsync",
            "-az",
            "--files-from",
            str(list_path),
            f"{REMOTE}:{run.remote_base}/slurm-logs/runsolver/",
            str(local / "runsolver") + "/",
        ]
    )


def parse_row_filename(path: Path) -> tuple[str, str, str] | None:
    if not path.stem.startswith("row-"):
        return None
    rest = path.stem[4:]
    parts = rest.split("-", 2)
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def read_instances(root: Path) -> list[dict[str, str]]:
    path = logs_root(root) / "instances.csv"
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def instance_maps(instances: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    by_id = {row["instance_id"]: row for row in instances}
    by_model_data: dict[str, str] = {}
    for row in instances:
        key = "\0".join([row.get("instance_relpath", ""), row.get("data_relpath_1", "")])
        by_model_data[key] = row["instance_id"]
    return by_id, by_model_data


def family_of(relpath: str) -> str:
    parts = (relpath or "").split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else "")


def rel_link(root: Path, path: Path | None, label: str) -> str:
    if path is None:
        return html.escape(label)
    rel = path.relative_to(root).as_posix()
    url = RAW_LOG_BASE + rel
    return f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>'


def read_single_csv_row(path: Path) -> list[str] | None:
    if path.stat().st_size == 0:
        return None
    with path.open(newline="") as handle:
        rows = list(csv.reader(handle))
    if len(rows) != 1:
        return None
    return rows[0]


def parse_values_log(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            continue
        out[key.strip()] = value.strip()
    return out


def classify_from_values(values: dict[str, str]) -> tuple[bool, bool]:
    status = values.get("ChildStatus", "") or values.get("SolverStatus", "")
    signal = values.get("Signal", "")
    wc = float(values.get("WCTime", values.get("WCTIME", "0")) or 0)
    cpu = float(values.get("CPUTotalTime", values.get("CPUTIME", "0")) or 0)
    memout = (
        status == "9"
        or values.get("OOM", "").lower() in {"1", "true", "yes"}
        or values.get("MEMOUT", "").lower() in {"1", "true", "yes"}
    )
    timeout = (
        signal in {"XCPU", "TERM"}
        or values.get("TIMEOUT", "").lower() in {"1", "true", "yes"}
        or wc >= 1199
        or cpu >= 1199
    )
    return timeout, memout


def parse_solver_log(path: Path | None) -> tuple[str, str]:
    if path is None or not path.exists():
        return "", ""
    status = ""
    objective = ""
    best = ""
    solution_markers = 0
    complete = False
    text = path.read_text(errors="replace")
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "----------":
            solution_markers += 1
        elif stripped in {"==========", "=====OPTIMAL====="}:
            complete = True
            if stripped == "=====OPTIMAL=====":
                status = "optimum"
        elif stripped in {"=====UNSATISFIABLE=====", "s UNSATISFIABLE"}:
            status = "unsat"
        elif stripped in {"=====SATISFIABLE=====", "s SATISFIABLE"}:
            status = status or "sat"
        elif stripped in {"=====UNKNOWN=====", "s UNKNOWN"}:
            status = status or "unknown"
        elif stripped.startswith("s "):
            token = stripped[2:].strip().lower()
            if token in {"sat", "satisfiable"}:
                status = status or "sat"
            elif token in {"unsat", "unsatisfiable"}:
                status = "unsat"
            elif token in {"optimum", "optimal"}:
                status = "optimum"
            elif token == "unknown":
                status = status or "unknown"

        match = re.match(r"^o\s+([-+]?\d+(?:\.\d+)?)\s*$", stripped)
        if match:
            objective = match.group(1)
        match = re.match(r"^%%\s*objective:\s*(\S+)\s*$", line)
        if match:
            objective = match.group(1).replace("'", "")
        match = re.search(r"\bbest:([^\s]+)", line)
        if match:
            best = match.group(1).strip("[]").replace("'", "")
        match = re.match(r"^\s*[A-Za-z0-9_]*objective[A-Za-z0-9_]*\s*=\s*([^;\s]+)", line)
        if match:
            objective = match.group(1).replace("'", "")

    if not objective and re.fullmatch(r"[-+]?\d+(?:\.\d+)?", best or ""):
        objective = best
    if not status:
        if complete and solution_markers:
            status = "optimum" if objective else "sat"
        elif solution_markers:
            status = "sat"
    return status, objective


def log_paths(root: Path, run: Run, row_path: Path, instance_id: str) -> tuple[Path | None, Path | None]:
    parsed = parse_row_filename(row_path)
    if not parsed:
        return None, None
    job, task, _ = parsed
    base = (
        logs_root(root)
        / run.slug
        / "runsolver"
        / f"{run.runsolver_prefix}-{job}-{task}-{instance_id}"
    )
    output = base / "output.log"
    values = base / "values.log"
    return (output if output.exists() else None, values if values.exists() else None)


def load_run_rows(
    root: Path,
    run: Run,
    instances: list[dict[str, str]],
) -> dict[str, dict[str, str | Path | None]]:
    by_id, by_model_data = instance_maps(instances)
    rows: dict[str, dict[str, str | Path | None]] = {}
    rows_dir = logs_root(root) / run.slug / "rows"
    for row_path in sorted(rows_dir.glob("*.csv")):
        parsed_name = parse_row_filename(row_path)
        name_instance_id = parsed_name[2] if parsed_name else ""
        raw = read_single_csv_row(row_path)
        if raw is None:
            instance_id = name_instance_id
            expected = by_id.get(instance_id, {})
            output_log, values_log = log_paths(root, run, row_path, instance_id)
            values = parse_values_log(values_log)
            timeout, memout = classify_from_values(values)
            rows[instance_id] = {
                "status": "empty",
                "objective": "",
                "row_path": row_path,
                "log_path": output_log,
                "values_path": values_log,
                "wctime": values.get("WCTime", values.get("WCTIME", "")),
                "cputime": values.get("CPUTotalTime", values.get("CPUTIME", "")),
                "maxrss": values.get("MaxMemory", values.get("MAXRSS", "")),
                "timeout": "true" if timeout else "",
                "memout": "true" if memout else "",
                "reason_code": "empty_row",
                "error": "",
                "family": family_of(expected.get("instance_relpath", "")),
            }
            continue

        instance_id = name_instance_id
        record: dict[str, str] = {}
        if run.solver == "ortools" and len(raw) == len(ORTOOLS_ROW_SCHEMA):
            record = dict(zip(ORTOOLS_ROW_SCHEMA, raw))
            instance_id = record["instance_id"] or instance_id
        elif run.solver == "kat" and len(raw) == len(KAT_ROW_SCHEMA):
            record = dict(zip(KAT_ROW_SCHEMA, raw))
            model = record.get("instance", "").removeprefix("/bench/")
            data = ""
            expected = by_id.get(instance_id, {})
            if not expected:
                # KAT rows contain only the model path, so the file name remains
                # the authoritative source for model+data instances.
                candidates = [row for row in instances if row["instance_id"] == instance_id]
                if candidates:
                    expected = candidates[0]
            if expected:
                key = "\0".join([expected.get("instance_relpath", model), expected.get("data_relpath_1", data)])
                instance_id = by_model_data.get(key, instance_id)
        expected = by_id.get(instance_id, {})
        output_log, values_log = log_paths(root, run, row_path, instance_id)
        log_status, log_objective = parse_solver_log(output_log)
        values = parse_values_log(values_log)
        timeout, memout = classify_from_values(values)

        if run.solver == "kat":
            status = record.get("status", "") or log_status or "unknown"
            objective = log_objective or record.get("root_lp_objective_value", "")
            total_ms = record.get("total_ms", "")
            wctime = f"{int(total_ms) / 1000:.3f}" if total_ms.isdigit() else values.get("WCTime", "")
            row = {
                "status": status,
                "objective": objective,
                "wctime": wctime,
                "cputime": values.get("CPUTotalTime", values.get("CPUTIME", "")),
                "maxrss": values.get("MaxMemory", values.get("MAXRSS", "")),
                "timeout": "true" if timeout or record.get("reason_code") == "timeout_signal" else "",
                "memout": "true" if memout else "",
                "reason_code": record.get("reason_code", ""),
                "error": record.get("error", ""),
            }
        else:
            status = log_status or record.get("status", "") or "unknown"
            objective = log_objective or record.get("objective", "")
            row = {
                "status": status,
                "objective": objective,
                "wctime": record.get("wctime", "") or values.get("WCTime", values.get("WCTIME", "")),
                "cputime": record.get("cputime", "") or values.get("CPUTotalTime", values.get("CPUTIME", "")),
                "maxrss": record.get("maxrss", "") or values.get("MaxMemory", values.get("MAXRSS", "")),
                "timeout": record.get("timeout", "") or ("true" if timeout else ""),
                "memout": record.get("memout", "") or ("true" if memout else ""),
                "reason_code": record.get("reason_code", ""),
                "error": record.get("error", ""),
            }

        row.update(
            {
                "row_path": row_path,
                "log_path": output_log,
                "values_path": values_log,
                "family": family_of(expected.get("instance_relpath", "")),
            }
        )
        rows[instance_id] = row
    runsolver_dir = logs_root(root) / run.slug / "runsolver"
    for output_log in sorted(runsolver_dir.glob(f"{run.runsolver_prefix}-*/output.log")):
        task_dir = output_log.parent.name
        rest = task_dir.removeprefix(run.runsolver_prefix + "-")
        parts = rest.split("-", 2)
        if len(parts) != 3:
            continue
        _job, _task, instance_id = parts
        if instance_id in rows:
            continue
        expected = by_id.get(instance_id, {})
        values_log = output_log.parent / "values.log"
        values = parse_values_log(values_log)
        timeout, memout = classify_from_values(values)
        log_status, log_objective = parse_solver_log(output_log)
        rows[instance_id] = {
            "status": log_status or "no_row",
            "objective": log_objective,
            "row_path": None,
            "log_path": output_log,
            "values_path": values_log if values_log.exists() else None,
            "wctime": values.get("WCTime", values.get("WCTIME", "")),
            "cputime": values.get("CPUTotalTime", values.get("CPUTIME", "")),
            "maxrss": values.get("MaxMemory", values.get("MAXRSS", "")),
            "timeout": "true" if timeout else "",
            "memout": "true" if memout else "",
            "reason_code": "no_row_file",
            "error": "",
            "family": family_of(expected.get("instance_relpath", "")),
        }
    return rows


def display_status(row: dict[str, str | Path | None] | None) -> str:
    if row is None:
        return "MISSING"
    status = str(row.get("status") or "unknown").lower()
    if status in {"optimum", "optimal"}:
        return "OPT"
    if status in {"sat", "satisfiable"}:
        return "SAT"
    if status in {"unsat", "unsatisfiable"}:
        return "UNSAT"
    if str(row.get("memout", "")).lower() == "true":
        return "MO"
    if str(row.get("timeout", "")).lower() == "true":
        return "TO"
    if status == "empty":
        return "EMPTY"
    if status == "no_row":
        return "NO_ROW"
    if str(row.get("error") or row.get("reason_code") or ""):
        return "ERR"
    return "UNKNOWN"


def build_summary(
    loaded: dict[str, dict[str, dict[str, str | Path | None]]],
    instances: list[dict[str, str]],
) -> str:
    labels = [run.column for run in RUNS]
    lines = [
        "<h2 id=\"summary\">Summary table</h2>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>run</th><th>row files</th><th>nonempty rows</th><th>OPT</th><th>SAT</th><th>UNSAT</th><th>solved</th><th>TO</th><th>MO</th><th>ERR</th><th>EMPTY</th><th>NO_ROW</th><th>MISSING</th><th>UNKNOWN</th></tr></thead>",
        "<tbody>",
    ]
    for label, run in zip(labels, RUNS):
        rows = loaded[run.slug]
        counts = Counter(display_status(rows.get(instance["instance_id"])) for instance in instances)
        row_files = len(list((logs_root(root_dir()) / run.slug / "rows").glob("*.csv")))
        nonempty = sum(1 for row in rows.values() if row.get("row_path") is not None and row.get("status") != "empty")
        solved = counts["OPT"] + counts["SAT"] + counts["UNSAT"]
        lines.append(
            "<tr>"
            f"<td>{html.escape(label)}</td>"
            f"<td>{row_files}</td><td>{nonempty}</td>"
            f"<td>{counts['OPT']}</td><td>{counts['SAT']}</td><td>{counts['UNSAT']}</td>"
            f"<td>{solved}</td><td>{counts['TO']}</td><td>{counts['MO']}</td>"
            f"<td>{counts['ERR']}</td><td>{counts['EMPTY']}</td>"
            f"<td>{counts['NO_ROW']}</td><td>{counts['MISSING']}</td><td>{counts['UNKNOWN']}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table></div>"])
    return "\n".join(lines)


def build_run_options() -> str:
    lines = [
        "<h2 id=\"run-options\">Run options</h2>",
        "<div class=\"table-wrap\"><table>",
        "<thead><tr><th>run</th><th>time / rsc</th><th>options</th></tr></thead>",
        "<tbody>",
    ]
    for run in RUNS:
        lines.append(
            "<tr>"
            f"<td>{html.escape(run.column)}</td>"
            f"<td><code>{html.escape(run.time_rsc)}</code></td>"
            f"<td><code>{html.escape(run.options)}</code></td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table></div>"])
    return "\n".join(lines)


def cell_html(root: Path, row: dict[str, str | Path | None] | None) -> str:
    label = display_status(row)
    if row is None:
        return html.escape(label)
    objective = str(row.get("objective") or "")
    wctime = str(row.get("wctime") or "")
    reason = str(row.get("reason_code") or row.get("error") or "")
    pieces = [label]
    if objective:
        pieces.append(objective)
    if wctime:
        pieces.append(f"{wctime}s")
    if reason and label in {"ERR", "EMPTY", "UNKNOWN", "TO", "MO"}:
        pieces.append(reason)
    text = " / ".join(pieces)
    path = row.get("log_path") or row.get("values_path") or row.get("row_path")
    return rel_link(root, path if isinstance(path, Path) else None, text)


def write_detail_csv(
    root: Path,
    loaded: dict[str, dict[str, dict[str, str | Path | None]]],
    instances: list[dict[str, str]],
) -> None:
    out = logs_root(root) / "summary_rows.csv"
    fields = [
        "instance_id",
        "family",
        "model",
        "data",
        "run",
        "status",
        "objective",
        "wctime",
        "cputime",
        "maxrss",
        "timeout",
        "memout",
        "reason_code",
        "error",
        "log",
    ]
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for instance in instances:
            for run in RUNS:
                row = loaded[run.slug].get(instance["instance_id"], {})
                log_path = row.get("log_path") if row else None
                writer.writerow(
                    {
                        "instance_id": instance["instance_id"],
                        "family": family_of(instance["instance_relpath"]),
                        "model": instance["instance_relpath"],
                        "data": instance.get("data_relpath_1", ""),
                        "run": run.column,
                        "status": display_status(row or None),
                        "objective": row.get("objective", "") if row else "",
                        "wctime": row.get("wctime", "") if row else "",
                        "cputime": row.get("cputime", "") if row else "",
                        "maxrss": row.get("maxrss", "") if row else "",
                        "timeout": row.get("timeout", "") if row else "",
                        "memout": row.get("memout", "") if row else "",
                        "reason_code": row.get("reason_code", "") if row else "",
                        "error": row.get("error", "") if row else "",
                        "log": log_path.relative_to(root).as_posix() if isinstance(log_path, Path) else "",
                    }
                )


def generate_html(root: Path) -> None:
    instances = read_instances(root)
    loaded = {run.slug: load_run_rows(root, run, instances) for run in RUNS}
    write_detail_csv(root, loaded, instances)

    row_lines: list[str] = []
    for idx, instance in enumerate(instances, 1):
        cells = [cell_html(root, loaded[run.slug].get(instance["instance_id"])) for run in RUNS]
        row_lines.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td><code>{html.escape(instance['instance_id'])}</code></td>"
            f"<td><code>{html.escape(family_of(instance['instance_relpath']))}</code></td>"
            f"<td><code>{html.escape(instance['instance_relpath'])}</code></td>"
            f"<td><code>{html.escape(instance.get('data_relpath_1', ''))}</code></td>"
            + "".join(f"<td>{cell}</td>" for cell in cells)
            + "</tr>"
        )

    body = "\n".join(
        [
            "<main>",
            "<h1 id=\"minizinc-results\">MiniZinc Challenge Results</h1>",
            f"<p>最終更新: {date.today().isoformat()}</p>",
            "<p>対象は MiniZinc Challenge official develop recursive list の 1801 instances。KAT と OR-Tools の 1200 秒 full sweep rows と runsolver logs を同期したもの。</p>",
            "<p>各 instance table のセルは solver stdout (<code>output.log</code>) へのリンク。リンクがない場合は row/values log のみ、または missing。</p>",
            build_summary(loaded, instances),
            build_run_options(),
            "<h2 id=\"instance-table\">Instance table</h2>",
            "<div class=\"table-wrap\"><table>",
            "<thead><tr><th>#</th><th>instance</th><th>family</th><th>model</th><th>data</th>"
            + "".join(f"<th>{html.escape(run.column)}</th>" for run in RUNS)
            + "</tr></thead>",
            "<tbody>",
            *row_lines,
            "</tbody>",
            "</table></div>",
            "</main>",
        ]
    )
    html_text = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MiniZinc Challenge Results</title>
<style>
:root {{ color-scheme: light; }}
body {{ margin: 0; padding: 2rem; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; line-height: 1.55; color: #1f2937; background: #f8fafc; }}
main {{ max-width: 1440px; margin: 0 auto; background: white; padding: 2rem; border: 1px solid #e5e7eb; border-radius: 12px; box-shadow: 0 1px 3px rgb(15 23 42 / 0.08); }}
h1, h2 {{ border-bottom: 1px solid #e5e7eb; padding-bottom: .25rem; }}
a {{ color: #2563eb; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
code {{ background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 4px; padding: 0 .25em; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .92em; }}
.table-wrap {{ overflow: auto; max-height: 82vh; margin: 1rem 0 2rem; border: 1px solid #d1d5db; }}
table {{ border-collapse: separate; border-spacing: 0; min-width: max-content; font-size: .9rem; }}
th, td {{ border-right: 1px solid #d1d5db; border-bottom: 1px solid #d1d5db; padding: .35rem .5rem; vertical-align: top; background: white; }}
th {{ position: sticky; top: 0; z-index: 3; background: #f1f5f9; }}
tr:nth-child(even) td {{ background: #fafafa; }}
</style>
</head>
<body>
{body}
</body>
</html>
"""
    out = root / "docs" / "minizinc.html"
    out.write_text(html_text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-fetch", action="store_true", help="Regenerate docs/minizinc.html from existing logs only")
    args = parser.parse_args()
    root = root_dir()
    if not args.no_fetch:
        fetch_instance_list(root)
        for run in RUNS:
            fetch_run(root, run)
    generate_html(root)


if __name__ == "__main__":
    main()
