#!/usr/bin/env python3
"""Fetch Laurel COP1000 logs and regenerate cop-results.md tables."""

from __future__ import annotations

import argparse
import csv
import re
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path

csv.field_size_limit(sys.maxsize)

REMOTE = "b39275@laurel.kudpc.kyoto-u.ac.jp"
REMOTE_BASE = "/LARGE0/gr10672/b39275/xcsp3instances"
BEGIN = "<!-- BEGIN COP1000_INSTANCE_TABLE -->"
END = "<!-- END COP1000_INSTANCE_TABLE -->"


@dataclass(frozen=True)
class Run:
    slug: str
    column: str
    result_dir: str
    log_kind: str
    runsolver_prefix: str = ""
    slurm_tag: str = ""
    remote_base: str = REMOTE_BASE


RUNS = [
    Run(
        "2c9fbcd6-order-ge-guarded-1800s",
        "2c9fbcd6 order-ge 1800s",
        "kat-cop22to25-1000-1800s-28c28g-p16-20260429-2c9fbcd6",
        "result-out-task",
    ),
    Run(
        "72769ef1-cumcegar-2m",
        "72769ef1 cumcegar 2m",
        "kat-cop22to25-1000-2m-cumcegar-20260429-72769ef1-dirty",
        "result-out-task",
    ),
    Run(
        "72769ef1-guarded-basic-2m",
        "72769ef1 guarded 2m",
        "kat-cop22to25-1000-2m-guarded-basic-20260429-72769ef1-dirty",
        "slurm-tag-output",
        slurm_tag="kat-cop22to25-1000-2m-guarded-basic-20260429-72769ef1-dirty",
    ),
    Run(
        "72769ef1-guarded-objvar-2m",
        "72769ef1 objvar 2m",
        "kat-cop22to25-1000-2m-guarded-basic-objvar-20260429-72769ef1-dirty",
        "slurm-tag-output",
        slurm_tag="kat-cop22to25-1000-2m-guarded-basic-objvar-20260429-72769ef1-dirty",
    ),
    Run(
        "1d1a452a-direct-order",
        "1d1a452a direct-order",
        "kat-cop22to25-1000-order-direct-mdd-tl-cop-20260501-1d1a452a-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop22to25-1000-order-direct-mdd-tl-cop-20260501-1d1a452a-q10672",
    ),
    Run(
        "1d1a452a-log-scop",
        "1d1a452a log-scop",
        "kat-cop22to25-1000-log-scop-mdd-tl-cop-20260501-1d1a452a-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop22to25-1000-log-scop-mdd-tl-cop-20260501-1d1a452a-q10672",
    ),
    Run(
        "a32997fd-direct-order",
        "a32997fd direct-order",
        "kat-cop22to25-1000-order-direct-mdd-tl-cop-20260503-a32997fd-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop22to25-1000-order-direct-mdd-tl-cop-20260503-a32997fd-q10672",
    ),
    Run(
        "partialdecode-20260504",
        "partialdecode 20260504",
        "kat-cop1000-decode-deaths100-order-direct-partialdecode-20260504",
        "runsolver",
        runsolver_prefix="runsolver-cop1000-decode100-partialdecode-20260504",
    ),
    Run(
        "31b6e7a3-scip-canary",
        "31b6e7a3 SCIP canary",
        "kat-cop1000-direct-order-mdd-tl-scip-static-20260504-31b6e7a3-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-static-20260504-31b6e7a3-q10672",
    ),
    Run(
        "31b6e7a3-scip-rerun1",
        "31b6e7a3 SCIP rerun1",
        "kat-cop1000-direct-order-mdd-tl-scip-static-20260504-31b6e7a3-q10672-rerun1",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-static-20260504-31b6e7a3-q10672-rerun1",
    ),
    Run(
        "31b6e7a3-scip-log-scop-rerun2",
        "31b6e7a3 SCIP log-scop rerun2",
        "kat-cop1000-log-scop-mdd-tl-scip-static-20260504-31b6e7a3-q10609-rerun2",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-scip-static-20260504-31b6e7a3-q10609-rerun2",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
]


def run_cmd(args: list[str]) -> None:
    print("+", " ".join(args))
    subprocess.run(args, check=True)


def fetch_logs(root: Path) -> None:
    logs = root / "logs"
    logs.mkdir(exist_ok=True)
    run_cmd(
        [
            "rsync",
            "-az",
            f"{REMOTE}:{REMOTE_BASE}/instance-lists/cop22to25_1000.csv",
            str(logs / "cop22to25_1000.csv"),
        ]
    )
    for run in RUNS:
        local = logs / run.slug
        (local / "rows").mkdir(parents=True, exist_ok=True)
        run_cmd(
            [
                "rsync",
                "-az",
                f"{REMOTE}:{run.remote_base}/results/{run.result_dir}/rows/",
                str(local / "rows") + "/",
            ]
        )
        if run.log_kind == "result-out-task":
            (local / "out").mkdir(exist_ok=True)
            run_cmd(
                [
                    "rsync",
                    "-az",
                    f"{REMOTE}:{run.remote_base}/results/{run.result_dir}/out/",
                    str(local / "out") + "/",
                ]
            )
        elif run.log_kind == "slurm-tag-output":
            (local / "slurm-logs").mkdir(exist_ok=True)
            run_cmd(
                [
                    "rsync",
                    "-az",
                    f"{REMOTE}:{run.remote_base}/slurm-logs/{run.slurm_tag}/",
                    str(local / "slurm-logs") + "/",
                ]
            )
        elif run.log_kind == "runsolver":
            dest = local / "runsolver"
            dest.mkdir(exist_ok=True)
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
                    str(dest) + "/",
                ]
            )


def read_instances(root: Path) -> list[tuple[int, str]]:
    path = root / "logs" / "cop22to25_1000.csv"
    if not path.exists():
        raise SystemExit(f"missing instance list: {path}")
    rows: list[tuple[int, str]] = []
    with path.open(newline="") as f:
        for raw in csv.reader(f):
            if not raw or raw[0] == "id":
                continue
            try:
                instance_id = int(raw[0])
            except ValueError:
                continue
            rows.append((instance_id, raw[-1]))
    rows.sort()
    return rows


def row_id(path: Path) -> int | None:
    m = re.match(r"task-(\d+)\.csv$", path.name)
    if m:
        return int(m.group(1))
    m = re.match(r"row-(\d+)-(\d+)-(\d+)\.csv$", path.name)
    if m:
        return int(m.group(3))
    return None


def load_rows(root: Path, run: Run) -> dict[int, tuple[Path, list[str]]]:
    rows: dict[int, tuple[Path, list[str]]] = {}
    for path in sorted((root / "logs" / run.slug / "rows").glob("*.csv")):
        instance_id = row_id(path)
        if instance_id is None:
            continue
        text = path.read_text(errors="replace").strip()
        fields = next(csv.reader([text])) if text else []
        rows[instance_id] = (path, fields)
    return rows


def log_path_for_row(root: Path, run: Run, instance_id: int, row_path: Path | None) -> Path | None:
    base = root / "logs" / run.slug
    if run.log_kind == "result-out-task":
        path = base / "out" / f"task-{instance_id}.out"
        return path if path.exists() else None
    if run.log_kind == "slurm-tag-output":
        if row_path is not None:
            m = re.match(r"row-(\d+)-(\d+)-(\d+)\.csv$", row_path.name)
            if m:
                path = base / "slurm-logs" / f"output-{m.group(1)}-{m.group(2)}-{m.group(3)}.log"
                if path.exists():
                    return path
        matches = sorted((base / "slurm-logs").glob(f"output-*-*-{instance_id}.log"))
        if matches:
            return matches[-1]
        matches = sorted((base / "slurm-logs").glob(f"slurm-*-{instance_id}.out"))
        return matches[-1] if matches else None
    if run.log_kind == "runsolver":
        if row_path is not None:
            m = re.match(r"row-(\d+)-(\d+)-(\d+)\.csv$", row_path.name)
            if m:
                path = (
                    base
                    / "runsolver"
                    / f"{run.runsolver_prefix}-{m.group(1)}-{m.group(2)}-{m.group(3)}"
                    / "output.log"
                )
                if path.exists():
                    return path
        matches = sorted((base / "runsolver").glob(f"{run.runsolver_prefix}-*-*-{instance_id}/output.log"))
        return matches[-1] if matches else None
    return None


def parse_log(log_path: Path | None) -> tuple[str | None, str | None, str | None]:
    if log_path is None or not log_path.exists():
        return None, None, None
    incumbent = None
    stage = None
    outcome = None
    text = log_path.read_text(errors="replace")
    lowered = text.lower()
    if any(token in lowered for token in ["out_of_memory", "oom", "killed", "exit code 137", "exit_code_137"]):
        outcome = "MO"
    if any(token in lowered for token in ["timeout", "time limit", "signal=15", "sigterm", "maximum cpu time"]):
        outcome = outcome or "TO"
    for line in text.splitlines():
        if line.startswith("o "):
            incumbent = line.split(None, 1)[1].strip()
        elif line.startswith("d OBJECTIVE_VALUE "):
            incumbent = line.rsplit(" ", 1)[-1].strip()
        elif "objective:incumbent" in line:
            m = re.search(r"value=(-?\d+)", line)
            if m:
                incumbent = m.group(1)
        if line.startswith("d CURRENT_STAGE "):
            stage = line.split(None, 2)[2].strip()
        elif line.startswith("d LAST_COMPLETED_STAGE ") and stage is None:
            stage = line.split(None, 2)[2].strip()
        else:
            m = re.search(r"\|\s+stage:([a-z0-9_-]+)", line)
            if m:
                stage = m.group(1)
    return incumbent, stage, outcome


def row_reason(fields: list[str]) -> tuple[str, str]:
    if not fields:
        return "", ""
    nonempty = [x for x in fields[2:] if x]
    if len(nonempty) >= 2:
        return nonempty[-2], nonempty[-1]
    if nonempty:
        return nonempty[-1], ""
    return "", ""


def classify_cell(
    fields: list[str],
    incumbent: str | None,
    stage: str | None,
    has_log: bool,
    log_outcome: str | None,
) -> str:
    status = fields[1] if len(fields) > 1 else ""
    reason, detail = row_reason(fields)
    if status == "unsat":
        return "UNSAT*"
    if incumbent is not None:
        return f"{incumbent}*" if status == "optimum" else incumbent
    if status == "optimum":
        return "OPT*"
    stop = stage or "unknown"
    reason_text = " ".join([reason, detail]).lower()
    if log_outcome in {"TO", "MO"}:
        return f"{log_outcome}({stop})"
    if "timeout" in reason_text or reason == "timeout_signal":
        return f"TO({stop})"
    if "137" in reason_text or not has_log or not fields:
        return f"MO({stop})"
    if status in {"internal_error", "parse_error"}:
        return f"ERR({stop})"
    return f"TO({stop})" if status == "unknown" else f"{status or 'NA'}({stop})"


def md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def rel_link(root: Path, path: Path | None, label: str) -> str:
    if path is None:
        return md_escape(label)
    rel = path.relative_to(root).as_posix()
    return f"[{md_escape(label)}]({rel})"


def missing_log(root: Path, run: Run, instance_id: int, instance: str, fields: list[str]) -> Path:
    path = root / "logs" / run.slug / "missing" / f"{instance_id}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    status = fields[1] if len(fields) > 1 else "missing"
    reason, detail = row_reason(fields)
    path.write_text(
        "\n".join(
            [
                f"run: {run.slug}",
                f"instance_id: {instance_id}",
                f"instance: {instance}",
                f"status: {status}",
                f"reason: {reason}",
                f"detail: {detail}",
                "",
                "No per-instance solver log was found for this cell.",
            ]
        )
        + "\n"
    )
    return path


def build_table(root: Path) -> str:
    instances = read_instances(root)
    all_rows = {run.slug: load_rows(root, run) for run in RUNS}
    lines = [
        BEGIN,
        "",
        "## COP1000 instance table",
        "",
        "Cell values are incumbent objective values. A trailing `*` means the run proved optimality.",
        "`TO(stage)` and `MO(stage)` mean no incumbent was found before timeout or memory-out at the indicated stage.",
        "Each cell links to the corresponding solver log when available.",
        "",
        "| # | instance | " + " | ".join(md_escape(run.column) for run in RUNS) + " |",
        "|---:|---|" + "|".join("---:" for _ in RUNS) + "|",
    ]
    for instance_id, instance in instances:
        row_cells = [str(instance_id), f"`{md_escape(Path(instance).name)}`"]
        for run in RUNS:
            row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
            log_path = log_path_for_row(root, run, instance_id, row_path)
            incumbent, stage, log_outcome = parse_log(log_path)
            label = classify_cell(fields, incumbent, stage, log_path is not None, log_outcome)
            if log_path is None:
                log_path = missing_log(root, run, instance_id, instance, fields)
            row_cells.append(rel_link(root, log_path, label))
        lines.append("| " + " | ".join(row_cells) + " |")
    lines.extend(["", END, ""])
    return "\n".join(lines)


def update_doc(root: Path) -> None:
    doc = root / "cop-results.md"
    table = build_table(root)
    if doc.exists():
        text = doc.read_text()
    else:
        text = "# COP Results\n\n"
    if BEGIN in text and END in text:
        pre = text.split(BEGIN, 1)[0].rstrip()
        post = text.split(END, 1)[1].lstrip()
        text = pre + "\n\n" + table + ("\n" + post if post else "")
    else:
        text = text.rstrip() + "\n\n" + table
    doc.write_text(text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-fetch", action="store_true", help="Regenerate from existing logs only")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not args.no_fetch:
        fetch_logs(root)
    update_doc(root)


if __name__ == "__main__":
    main()
