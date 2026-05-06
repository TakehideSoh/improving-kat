#!/usr/bin/env python3
"""Fetch Laurel COP1000 logs and regenerate cop-results.md tables."""

from __future__ import annotations

import argparse
import csv
import re
import sys
import subprocess
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

csv.field_size_limit(sys.maxsize)

REMOTE = "b39275@laurel.kudpc.kyoto-u.ac.jp"
REMOTE_BASE = "/LARGE0/gr10672/b39275/xcsp3instances"
BEGIN = "<!-- BEGIN COP1000_INSTANCE_TABLE -->"
END = "<!-- END COP1000_INSTANCE_TABLE -->"
VALIDATION_BEGIN = "<!-- BEGIN COP1000_VALIDATION_STATS -->"
VALIDATION_END = "<!-- END COP1000_VALIDATION_STATS -->"
DEFAULT_CHECKER_JAR = Path(
    "/home/soh/02_prog/xcsp3instances/XCSP3-Java-Tools/target/xcsp3-solutionChecker-2.6.0.jar"
)
DEFAULT_BENCHMARK_DIR = Path("/home/soh/02_prog/benchmark")


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
    Run(
        "9c976720-scip-log-scop-dynamic",
        "9c976720 SCIP log-scop dynamic",
        "kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260505-9c976720-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260505-9c976720-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "9c976720-scip-direct-order-dynamic",
        "9c976720 SCIP direct-order dynamic",
        "kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260505-9c976720-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260505-9c976720-q10672",
    ),
    Run(
        "bd3c9f7d-scip-direct-order-dynamic",
        "bd3c9f7d SCIP direct-order dynamic",
        "kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260506-bd3c9f7d-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260506-bd3c9f7d-q10672",
    ),
    Run(
        "pycsp3-extra-ortools-20260505",
        "pycsp3-extra OR-Tools 20260505",
        "pycsp3-extra-ortools-cop1000-20260505-q10609",
        "runsolver",
        runsolver_prefix="runsolver-pycsp3-extra-ortools-cop1000-20260505-q10609",
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


def extract_solution_text(log_path: Path) -> str | None:
    lines: list[str] = []
    in_solution = False
    for line in log_path.read_text(errors="replace").splitlines():
        if not line.startswith("v "):
            continue
        payload = line[2:]
        if payload.startswith("<instantiation"):
            lines = []
            in_solution = True
        if in_solution:
            lines.append(payload)
        if in_solution and payload.startswith("</instantiation"):
            return "\n".join(lines) + "\n"
    return None


def instance_path(benchmark_dir: Path, instance: str) -> Path:
    marker = "/competition/"
    if marker in instance:
        return benchmark_dir / "competition" / instance.split(marker, 1)[1]
    return benchmark_dir / instance.lstrip("/")


def classify_checker_output(returncode: int, output: str) -> tuple[str, str]:
    detail = output.strip().splitlines()[-1] if output.strip() else f"exit={returncode}"
    lowered = output.lower()
    if "exception" in lowered or "fatal error" in lowered:
        return "checker_error", detail
    if returncode == 0 and "ok" in lowered and "invalid" not in lowered and "incorrect" not in lowered:
        return "valid", detail
    if "invalid" in lowered or "incorrect" in lowered or "violation" in lowered:
        return "invalid", detail
    if returncode != 0:
        return "checker_error", detail
    return "valid", detail


def load_validation(root: Path) -> dict[tuple[str, int], str]:
    path = root / "logs" / "validation" / "results.csv"
    if not path.exists():
        return {}
    out: dict[tuple[str, int], str] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                out[(row["run"], int(row["instance_id"]))] = row["validation"]
            except (KeyError, ValueError):
                continue
    return out


def validate_solutions(root: Path, checker_jar: Path, benchmark_dir: Path, workers: int) -> None:
    if not checker_jar.exists():
        raise SystemExit(f"solution checker jar not found: {checker_jar}")
    instances = dict(read_instances(root))
    all_rows = {run.slug: load_rows(root, run) for run in RUNS}
    validation_dir = root / "logs" / "validation"
    output_dir = validation_dir / "checker-output"
    output_dir.mkdir(parents=True, exist_ok=True)
    def validate_one(run: Run, instance_id: int, instance: str) -> dict[str, str]:
        row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
        log_path = log_path_for_row(root, run, instance_id, row_path)
        status = fields[1] if len(fields) > 1 else ""
        incumbent, _, _ = parse_log(log_path)
        validation = "skipped_no_incumbent"
        detail = ""
        if status == "unsat":
            validation = "skipped_unsat"
        elif incumbent is not None:
            if log_path is None:
                validation = "no_log"
            else:
                solution = extract_solution_text(log_path)
                if solution is None:
                    validation = "no_solution"
                else:
                    inst = instance_path(benchmark_dir, instance)
                    if not inst.exists():
                        validation = "missing_instance"
                        detail = str(inst)
                    else:
                        out_path = output_dir / run.slug / f"{instance_id}.txt"
                        if out_path.exists() and out_path.stat().st_mtime >= log_path.stat().st_mtime:
                            validation, detail = classify_checker_output(0, out_path.read_text(errors="replace"))
                        else:
                            with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False) as tmp:
                                tmp.write(solution)
                                solution_path = Path(tmp.name)
                            try:
                                proc = subprocess.run(
                                    ["java", "-jar", str(checker_jar), str(inst), str(solution_path)],
                                    text=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    timeout=120,
                                )
                                output = proc.stdout
                                out_path.parent.mkdir(parents=True, exist_ok=True)
                                out_path.write_text(output)
                                validation, detail = classify_checker_output(proc.returncode, output)
                            except subprocess.TimeoutExpired as e:
                                validation = "checker_timeout"
                                detail = str(e)
                            finally:
                                solution_path.unlink(missing_ok=True)
        return {
            "run": run.slug,
            "instance_id": str(instance_id),
            "instance": instance,
            "status": status,
            "incumbent": incumbent or "",
            "validation": validation,
            "detail": detail,
        }

    tasks = [(run, instance_id, instance) for run in RUNS for instance_id, instance in instances.items()]
    rows: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(validate_one, *task) for task in tasks]
        for i, future in enumerate(as_completed(futures), 1):
            rows.append(future.result())
            if i % 500 == 0:
                print(f"validated {i}/{len(futures)} cells", file=sys.stderr)
    rows.sort(key=lambda row: (row["run"], int(row["instance_id"])))
    path = validation_dir / "results.csv"
    with path.open("w", newline="") as f:
        fieldnames = ["run", "instance_id", "instance", "status", "incumbent", "validation", "detail"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def decorate_validation(label: str, validation: str | None) -> str:
    if validation in {"invalid", "checker_timeout"}:
        return f"INVALID {label}"
    if validation == "checker_error":
        return f"CHECKER_ERROR {label}"
    return label


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
    validation = load_validation(root)
    lines = [
        BEGIN,
        "",
        "## COP1000 instance table",
        "",
        "Cell values are incumbent objective values. A trailing `*` means the run proved optimality.",
        "`TO(stage)` and `MO(stage)` mean no incumbent was found before timeout or memory-out at the indicated stage.",
        "Each cell links to the corresponding solver log when available.",
        "`INVALID` marks a solution rejected by validation; `CHECKER_ERROR` marks a checker failure such as an unsupported solution variable.",
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
            label = decorate_validation(label, validation.get((run.slug, instance_id)))
            if log_path is None:
                log_path = missing_log(root, run, instance_id, instance, fields)
            row_cells.append(rel_link(root, log_path, label))
        lines.append("| " + " | ".join(row_cells) + " |")
    lines.extend(["", END, ""])
    return "\n".join(lines)


def build_validation_stats(root: Path) -> str:
    validation = load_validation(root)
    lines = [
        VALIDATION_BEGIN,
        "",
        "## Validation stats",
        "",
        "Validation uses `xcsp3-solutionChecker-2.6.0.jar` on cells with an incumbent and a solver log solution.",
        "",
        "| run | valid | invalid | checker_error | no_solution | skipped_unsat | skipped_no_incumbent | missing_instance | checker_timeout |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for run in RUNS:
        counts = Counter(v for (slug, _), v in validation.items() if slug == run.slug)
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{run.slug}`",
                    str(counts["valid"]),
                    str(counts["invalid"]),
                    str(counts["checker_error"]),
                    str(counts["no_solution"]),
                    str(counts["skipped_unsat"]),
                    str(counts["skipped_no_incumbent"]),
                    str(counts["missing_instance"]),
                    str(counts["checker_timeout"]),
                ]
            )
            + " |"
        )
    lines.extend(["", VALIDATION_END, ""])
    return "\n".join(lines)


def update_doc(root: Path) -> None:
    doc = root / "cop-results.md"
    table = build_table(root)
    validation_stats = build_validation_stats(root)
    if doc.exists():
        text = doc.read_text()
    else:
        text = "# COP Results\n\n"
    if VALIDATION_BEGIN in text and VALIDATION_END in text:
        pre = text.split(VALIDATION_BEGIN, 1)[0].rstrip()
        post = text.split(VALIDATION_END, 1)[1].lstrip()
        text = pre + "\n\n" + validation_stats + ("\n" + post if post else "")
    else:
        marker = BEGIN if BEGIN in text else None
        if marker:
            pre, post = text.split(marker, 1)
            text = pre.rstrip() + "\n\n" + validation_stats + "\n" + marker + post
        else:
            text = text.rstrip() + "\n\n" + validation_stats
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
    parser.add_argument("--validate", action="store_true", help="Run XCSP3 solutionChecker before regenerating")
    parser.add_argument("--checker-jar", type=Path, default=DEFAULT_CHECKER_JAR)
    parser.add_argument("--benchmark-dir", type=Path, default=DEFAULT_BENCHMARK_DIR)
    parser.add_argument("--validation-workers", type=int, default=4)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not args.no_fetch:
        fetch_logs(root)
    if args.validate:
        validate_solutions(root, args.checker_jar, args.benchmark_dir, args.validation_workers)
    update_doc(root)


if __name__ == "__main__":
    main()
