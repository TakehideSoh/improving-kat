#!/usr/bin/env python3
"""Fetch Laurel COP1000 logs and regenerate cop-results.md tables."""

from __future__ import annotations

import argparse
import csv
import lzma
import re
import sys
import subprocess
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path

csv.field_size_limit(sys.maxsize)

REMOTE = "b39275@laurel.kudpc.kyoto-u.ac.jp"
REMOTE_BASE = "/LARGE0/gr10672/b39275/xcsp3instances"
BEGIN = "<!-- BEGIN COP1000_INSTANCE_TABLE -->"
END = "<!-- END COP1000_INSTANCE_TABLE -->"
VALIDATION_BEGIN = "<!-- BEGIN COP1000_VALIDATION_STATS -->"
VALIDATION_END = "<!-- END COP1000_VALIDATION_STATS -->"
CONSISTENCY_BEGIN = "<!-- BEGIN COP1000_CONSISTENCY_STATS -->"
CONSISTENCY_END = "<!-- END COP1000_CONSISTENCY_STATS -->"
REFERENCE_COMPARE_BEGIN = "<!-- BEGIN COP1000_REFERENCE_COMPARISON_STATS -->"
REFERENCE_COMPARE_END = "<!-- END COP1000_REFERENCE_COMPARISON_STATS -->"
SUMMARY_BEGIN = "<!-- BEGIN COP1000_RESULT_SUMMARY -->"
SUMMARY_END = "<!-- END COP1000_RESULT_SUMMARY -->"
NOINC_SUBSET_BEGIN = "<!-- BEGIN COP1000_NOINC_SUBSET_SUMMARY -->"
NOINC_SUBSET_END = "<!-- END COP1000_NOINC_SUBSET_SUMMARY -->"
NOINC_SUBSET_CSV = "cop1000_no_incumbent_203_instance_list.csv"
NOINC_DIAGNOSIS_CSV = "cop1000_noinc203_reach_sat_diagnosis.csv"
NOINC_SACCT_CSV = "cop1000_noinc203_sacct_19442938.csv"
NOINC_BASELINE_RUN = "1173b3f4-direct-order-extprop8196-s40-20260512"
NOINC_TARGET_RUN = "10c9c43b-dirty-noinc203-cegar-norootlp"
DOC_RUN_SLUGS = [
    "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase",
    "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd",
    "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd",
    "ace64g-rr-20260505",
    "pycsp3-extra-ortools-20260505",
    "pycsp3-extra-ortools-1t-verbose1-20260509",
]
NOINC_SUBSET_RUNS = [
    slug
    for slug in DOC_RUN_SLUGS
    if slug
    not in {
        "b85de3f1-dirty-log-scop-mdd-tl-directexpr-norootlp",
        "pycsp3-extra-ortools-1t-verbose1-20260509",
    }
]
CONSISTENCY_RUNS = [
    "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase",
    "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd",
    "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd",
    "ace64g-rr-20260505",
    "pycsp3-extra-ortools-20260505",
    "pycsp3-extra-ortools-1t-verbose1-20260509",
]
DEFAULT_CHECKER_JAR = Path(
    "/home/soh/02_prog/xcsp3instances/XCSP3-Java-Tools/target/xcsp3-solutionChecker-2.6.0.jar"
)
DEFAULT_BENCHMARK_DIR = Path("/home/soh/02_prog/benchmark")
BAD_VALIDATION = {"invalid", "checker_error", "checker_timeout"}
REFERENCE_COMPARE_TARGET = (
    "b85de3f1-dirty-direct-order-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp"
)
REFERENCE_COMPARE_RUNS = [
    "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
    "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase",
    "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd",
    "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd",
    "ace64g-rr-20260505",
    "pycsp3-extra-ortools-20260505",
    "pycsp3-extra-ortools-1t-verbose1-20260509",
]


@dataclass(frozen=True)
class Run:
    slug: str
    column: str
    result_dir: str
    log_kind: str
    runsolver_prefix: str = ""
    slurm_tag: str = ""
    remote_base: str = REMOTE_BASE


COP1000_COLUMN_ALIASES = {
    "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd": "bacAU",
    "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd": "2c52AU",
    "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd": "ee83PS",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd": "630PS",
    "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase": "630NP",
    "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd": "2ccOrd",
    "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd": "0d68Lg",
    "ace64g-rr-20260505": "ACE64G",
    "pycsp3-extra-ortools-20260505": "ORT28",
    "pycsp3-extra-ortools-1t-verbose1-20260509": "ORT1V",
}


def logs_root(root: Path) -> Path:
    return root / "docs" / "logs"


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
        "bd3c9f7d-scip-log-scop-dynamic",
        "bd3c9f7d SCIP log-scop dynamic",
        "kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260506-bd3c9f7d-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260506-bd3c9f7d-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "aac6b714-scip-direct-order-dynamic",
        "aac6b714 SCIP direct-order dynamic",
        "kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260507-aac6b714-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260507-aac6b714-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "835f8aaf-scip-direct-order-dynamic",
        "835f8aaf SCIP direct-order dynamic",
        "kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260508-835f8aaf-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-dynamic-20260508-835f8aaf-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "835f8aaf-scip-log-scop-dynamic",
        "835f8aaf SCIP log-scop dynamic",
        "kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260508-835f8aaf-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-scip-dynamic-20260508-835f8aaf-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "10c9c43b-dirty-log-scop-dadda-norootlp",
        "10c9c43b-dirty log-scop dadda no-root-lp",
        "kat-cop1000-log-scop-mdd-tl-dadda-norootlp-20260515-10c9c43b-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-dadda-norootlp-20260515-10c9c43b-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "10c9c43b-dirty-log-scop-autobdd-norootlp-rerun1",
        "10c9c43b-dirty log-scop autoBDD no-root-lp rerun1",
        "kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260515-10c9c43b-dirty-q10609-rerun1",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260515-10c9c43b-dirty-q10609-rerun1",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "a5249e5b-log-scop-autobdd-norootlp",
        "a5249e5b log-scop autoBDD no-root-lp",
        "kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260516-a5249e5b-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260516-a5249e5b-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "f85951b8-log-scop-autobdd-norootlp",
        "f85951b8 log-scop autoBDD no-root-lp",
        "kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260517-f85951b8-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-autobdd-norootlp-20260517-f85951b8-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "1173b3f4-scip-direct-order-extge-eager-dynwatch",
        "1173b3f4 SCIP direct-order extge eager dynwatch",
        "kat-cop1000-direct-order-mdd-tl-scip-extge-eager-dynwatch-20260511-1173b3f4-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-scip-extge-eager-dynwatch-20260511-1173b3f4-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "1173b3f4-direct-order-extprop8196-s40-20260512",
        "1173b3f4 direct-order extprop8196 s40",
        "kat-cop1000-direct-order-mdd-tl-extprop-dp8196-s40-20260512-1173b3f4-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop-dp8196-s40-20260512-1173b3f4-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "261d4b72-direct-order-extprop8196-s40-extobj-norootlp-nomaxarity",
        "[BUG] 261d4b72 direct-order extprop8196 s40 extobj no-root-lp no max-arity",
        "kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-norootlp-nomaxarity-20260516-261d4b72-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-norootlp-nomaxarity-20260516-261d4b72-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "f85951b8-direct-order-extprop8196-s40-extobj-norootlp-nomaxarity",
        "[BUG] f85951b8 direct-order extprop8196 s40 extobj no-root-lp no max-arity",
        "kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-norootlp-nomaxarity-20260517-f85951b8-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-norootlp-nomaxarity-20260517-f85951b8-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "c70e1a64-direct-order-extprop8196-s40-extobj-linkcost-maxarity2-norootlp",
        "c70e1a64 direct-order extprop8196 s40 extobj link-cost maxarity2 no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-linkcost-maxarity2-norootlp-20260519-c70e1a64-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop8196-s40-extobj-linkcost-maxarity2-norootlp-20260519-c70e1a64-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "c70e1a64-direct-order-extprop8196-s40-linkcost-maxarity2-norootlp",
        "c70e1a64 direct-order extprop8196 s40 link-cost maxarity2 no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-extprop8196-s40-linkcost-maxarity2-norootlp-20260519-c70e1a64-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop8196-s40-linkcost-maxarity2-norootlp-20260519-c70e1a64-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "489538e3-direct-order-extprop8196-s40-directexpr-linkcost-maxarity2-norootlp",
        "489538e3 direct-order extprop8196 s40 directexpr link-cost maxarity2 no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-extprop8196-s40-directexpr-linkcost-maxarity2-norootlp-20260519-489538e3-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-extprop8196-s40-directexpr-linkcost-maxarity2-norootlp-20260519-489538e3-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "5362e21a-direct-order-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp",
        "5362e21a direct-order memguard64 extfallback directexpr link-cost maxarity2 no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp-20260525-5362e21a-q10672",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp-20260525-5362e21a-q10672",
    ),
    Run(
        "2ccb88e9-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd",
        "2ccb88e9 order-ge guarded-basic directexpr inchard cnf mdd-tl link-cost eqne2 cumexact time-rd",
        "kat-cop1000-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd-20260526-2ccb88e9-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-order-ge-guarded-basic-directexpr-inchard-cnf-mdd-tl-linkcost-eqne2-cumexact-timerd-20260526-2ccb88e9-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "dbb61f6f-dirty-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
        "dbb61f6f-dirty portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260529-dbb61f6f-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260529-dbb61f6f-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "1dbef8ce-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
        "1dbef8ce portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260529-1dbef8ce-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260529-1dbef8ce-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "1dbef8ce-portfolio-v3-bump-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
        "1dbef8ce portfolio-v3 bump guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-bump-20260529-1dbef8ce-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-bump-20260529-1dbef8ce-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "f3be10ca-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
        "f3be10ca portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objguided phase-save cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd-20260601-f3be10ca-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd-20260601-f3be10ca-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "bac40e3b-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
        "bac40e3b portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-bac40e3b-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-bac40e3b-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "2c5208ae-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd",
        "2c5208ae portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objauto cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-2c5208ae-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objauto-cumexact-timerd-20260603-2c5208ae-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "ee83b5a8-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd",
        "ee83b5a8 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge objguided phase-save cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd-20260602-ee83b5a8-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-objguided-phasesave-cumexact-timerd-20260602-ee83b5a8-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd",
        "630ede96 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260531-630ede96-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-20260531-630ede96-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "630ede96-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase",
        "630ede96 portfolio-v3 guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg link-cost eqne2 logEq->ge cumexact time-rd no-phase",
        "kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase-20260531-630ede96-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-portfolio-v3-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-linkcost-eqne2-logeqge-cumexact-timerd-nophase-20260531-630ede96-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "b85de3f1-dirty-direct-order-memguard64-extfallback-directexpr",
        "b85de3f1-dirty direct-order memguard64 extfallback directexpr",
        "kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-20260521-b85de3f1-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-20260521-b85de3f1-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "b85de3f1-dirty-direct-order-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp",
        "b85de3f1-dirty direct-order memguard64 extfallback directexpr link-cost maxarity2 no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp-20260521-b85de3f1-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-linkcost-maxarity2-norootlp-20260521-b85de3f1-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "b85de3f1-dirty-direct-order-memguard64-extfallback-directexpr-norootlp",
        "b85de3f1-dirty direct-order memguard64 extfallback directexpr no-root-lp",
        "kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-norootlp-20260522-b85de3f1-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-direct-order-mdd-tl-memguard64-extfallback-directexpr-norootlp-20260522-b85de3f1-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "b85de3f1-dirty-log-scop-mdd-tl-directexpr-norootlp",
        "b85de3f1-dirty log-scop mdd-tl directexpr no-root-lp",
        "kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-20260521-b85de3f1-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-20260521-b85de3f1-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "2ccb88e9-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd",
        "2ccb88e9 log-scop guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg eqne2 cumexact time-rd",
        "kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd-20260526-2ccb88e9-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd-20260526-2ccb88e9-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd",
        "0d68ca9c-dirty log-scop guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg eqne2 cumexact time-rd",
        "kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd-20260528-0d68ca9c-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-eqne2-cumexact-timerd-20260528-0d68ca9c-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "0d68ca9c-dirty-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd",
        "0d68ca9c-dirty log-scop guarded-basic directexpr inchard cnf mdd-tl autoPB autoBDD agg cumexact time-rd",
        "kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd-20260529-0d68ca9c-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-guarded-basic-directexpr-inchard-cnf-mdd-tl-autopb-autobdd-agg-cumexact-timerd-20260529-0d68ca9c-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "6b92cd56-log-scop-mdd-tl-directexpr-norootlp-cumext-boundprop",
        "6b92cd56 log-scop mdd-tl directexpr no-root-lp cumext boundprop",
        "kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-cumext-boundprop-20260524-6b92cd56-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-cumext-boundprop-20260524-6b92cd56-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "243acd4c-log-scop-mdd-tl-directexpr-norootlp",
        "243acd4c log-scop mdd-tl directexpr no-root-lp",
        "kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-20260522-243acd4c-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-log-scop-mdd-tl-directexpr-norootlp-20260522-243acd4c-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "10c9c43b-dirty-noinc203-cegar-norootlp",
        "10c9c43b-dirty noinc203 CEGAR no-root-lp",
        "kat-cop1000-noinc203-order-direct-mdd-tl-extprop8196-s40-cegar-norootlp-20260513-10c9c43b-dirty-q10609",
        "runsolver",
        runsolver_prefix="runsolver-kat-cop1000-noinc203-order-direct-mdd-tl-extprop8196-s40-cegar-norootlp-20260513-10c9c43b-dirty-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "ace64g-rr-20260505",
        "ACE 64G rr 20260505",
        "",
        "runsolver-flat",
        runsolver_prefix="runsolver-ace64g-rr-cop1000-20260505-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "pycsp3-extra-ortools-20260505",
        "pycsp3-extra OR-Tools 28t 20260505",
        "pycsp3-extra-ortools-cop1000-20260505-q10609",
        "runsolver",
        runsolver_prefix="runsolver-pycsp3-extra-ortools-cop1000-20260505-q10609",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "pycsp3-extra-ortools-1t-rerun1-20260505",
        "pycsp3-extra OR-Tools 1t 20260505",
        "pycsp3-extra-ortools-cop1000-20260505-q10609-1t-rerun1",
        "runsolver",
        runsolver_prefix="runsolver-pycsp3-extra-ortools-cop1000-20260505-q10609-1t-rerun1",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
    Run(
        "pycsp3-extra-ortools-1t-verbose1-20260509",
        "pycsp3-extra OR-Tools 1t verbose1 20260509",
        "pycsp3-extra-ortools-cop1000-verbose1-20260509-q10609-1t",
        "runsolver",
        runsolver_prefix="runsolver-pycsp3-extra-ortools-cop1000-verbose1-20260509-q10609-1t",
        remote_base="/LARGE0/gr10609/b39275/xcsp3instances",
    ),
]

DOC_RUNS = [run for run in RUNS if run.slug in DOC_RUN_SLUGS]


def run_cmd(args: list[str]) -> None:
    print("+", " ".join(args))
    subprocess.run(args, check=True)


def fetch_logs(root: Path) -> None:
    logs = logs_root(root)
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
        local.mkdir(parents=True, exist_ok=True)
        if run.result_dir:
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
        elif run.log_kind == "runsolver-flat":
            dest = local / "runsolver"
            dest.mkdir(exist_ok=True)
            run_cmd(
                [
                    "rsync",
                    "-az",
                    "--prune-empty-dirs",
                    f"--include={run.runsolver_prefix}-[0-9]*-[0-9]*-[0-9]*.out",
                    f"--include={run.runsolver_prefix}-[0-9]*-[0-9]*-[0-9]*.var",
                    "--exclude=*",
                    f"{REMOTE}:{run.remote_base}/slurm-logs/runsolver/",
                    str(dest) + "/",
                ]
            )


def read_instances(root: Path) -> list[tuple[int, str]]:
    path = logs_root(root) / "cop22to25_1000.csv"
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


def flat_runsolver_row_id(path: Path, prefix: str) -> int | None:
    escaped = re.escape(prefix)
    m = re.match(rf"{escaped}-(\d+)-(\d+)-(\d+)\.out$", path.name)
    if m:
        return int(m.group(3))
    return None


def status_from_log_text(text: str) -> str:
    if re.search(r"^s\s+UNSAT", text, re.MULTILINE):
        return "unsat"
    if re.search(r"^s\s+OPTIMUM", text, re.MULTILINE):
        return "optimum"
    if re.search(r"^s\s+SAT", text, re.MULTILINE):
        if "d COMPLETE EXPLORATION" in text and "d INCOMPLETE EXPLORATION" not in text:
            return "optimum"
        return "sat"
    return "unknown"


def load_rows(root: Path, run: Run) -> dict[int, tuple[Path, list[str]]]:
    rows: dict[int, tuple[Path, list[str]]] = {}
    if run.log_kind == "runsolver-flat":
        for path in sorted((logs_root(root) / run.slug / "runsolver").glob(f"{run.runsolver_prefix}-*.out")):
            instance_id = flat_runsolver_row_id(path, run.runsolver_prefix)
            if instance_id is None:
                continue
            text = path.read_text(errors="replace")
            rows[instance_id] = (path, ["", status_from_log_text(text)])
        return rows
    for path in sorted((logs_root(root) / run.slug / "rows").glob("*.csv")):
        instance_id = row_id(path)
        if instance_id is None:
            continue
        text = path.read_text(errors="replace").strip()
        fields = next(csv.reader([text])) if text else []
        rows[instance_id] = (path, fields)
    return rows


def log_path_for_row(root: Path, run: Run, instance_id: int, row_path: Path | None) -> Path | None:
    base = logs_root(root) / run.slug
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
    if run.log_kind == "runsolver-flat":
        if row_path is not None and row_path.suffix == ".out" and row_path.exists():
            return row_path
        matches = sorted((base / "runsolver").glob(f"{run.runsolver_prefix}-*-*-{instance_id}.out"))
        return matches[-1] if matches else None
    return None


def runsolver_values_path(log_path: Path) -> Path | None:
    if log_path.name == "output.log":
        path = log_path.with_name("values.log")
        return path if path.exists() else None
    if log_path.suffix == ".out":
        path = log_path.with_suffix(".var")
        return path if path.exists() else None
    return None


@lru_cache(maxsize=None)
def parse_log(log_path: Path | None) -> tuple[str | None, str | None, str | None]:
    if log_path is None or not log_path.exists():
        return None, None, None
    incumbent = None
    stage = None
    outcome = None
    text = log_path.read_text(errors="replace")
    lowered = text.lower()
    values_path = runsolver_values_path(log_path)
    if values_path is not None:
        values = values_path.read_text(errors="replace").lower()
        if "memout=true" in values:
            outcome = "MO"
        if "timeout=true" in values:
            outcome = outcome or "TO"
    if any(token in lowered for token in ["out_of_memory", "oom", "killed", "exit code 137", "exit_code_137"]):
        outcome = "MO"
    if any(token in lowered for token in ["timeout", "time limit", "signal=15", "sigterm", "maximum cpu time"]):
        outcome = outcome or "TO"
    for line in text.splitlines():
        if line.startswith("o "):
            incumbent = line.split(None, 2)[1].replace(",", "").strip()
        elif line.startswith("d OBJECTIVE_VALUE "):
            incumbent = line.rsplit(" ", 1)[-1].strip()
        elif line.startswith("d BOUND "):
            m = re.search(r"d BOUND\s+(-?[\d,]+)", line)
            if m:
                incumbent = m.group(1).replace(",", "")
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
        if line.startswith("d INCOMPLETE EXPLORATION"):
            stage = stage or "search"
            outcome = outcome or "TO"
    return incumbent, stage, outcome


def extract_solution_text(log_path: Path) -> str | None:
    lines: list[str] = []
    in_solution = False
    for line in log_path.read_text(errors="replace").splitlines():
        if not line.startswith("v "):
            continue
        payload = line[2:]
        stripped = payload.strip()
        if stripped.startswith("<instantiation"):
            lines = []
            in_solution = True
        if in_solution:
            lines.append(stripped)
        if in_solution and "</instantiation" in stripped:
            return "\n".join(lines) + "\n"
        if in_solution and stripped.startswith("</instantiation"):
            return "\n".join(lines) + "\n"
    return None


def instance_path(benchmark_dir: Path, instance: str) -> Path:
    marker = "/competition/"
    if marker in instance:
        return benchmark_dir / "competition" / instance.split(marker, 1)[1]
    return benchmark_dir / instance.lstrip("/")


@lru_cache(maxsize=None)
def objective_sense(benchmark_dir: Path, instance: str) -> str:
    path = instance_path(benchmark_dir, instance)
    if not path.exists():
        return "unknown"
    try:
        data = path.read_bytes()
        if path.suffix == ".lzma":
            data = lzma.decompress(data)
        text = data.decode("utf-8", errors="ignore")
    except (OSError, lzma.LZMAError):
        return "unknown"
    m = re.search(r"<\s*(minimize|maximize)\b", text)
    return m.group(1) if m else "unknown"


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


VALIDATION_FIELDNAMES = [
    "run",
    "instance_id",
    "instance",
    "status",
    "incumbent",
    "validation",
    "detail",
    "log_mtime",
]


def load_validation_rows(root: Path) -> dict[tuple[str, int], dict[str, str]]:
    path = logs_root(root) / "validation" / "results.csv"
    if not path.exists():
        return {}
    out: dict[tuple[str, int], dict[str, str]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                out[(row["run"], int(row["instance_id"]))] = row
            except (KeyError, ValueError):
                continue
    return out


def load_validation(root: Path) -> dict[tuple[str, int], str]:
    return {key: row.get("validation", "") for key, row in load_validation_rows(root).items()}


def log_mtime(log_path: Path | None) -> str:
    if log_path is None or not log_path.exists():
        return ""
    return str(log_path.stat().st_mtime_ns)


def reusable_validation_row(existing: dict[str, str] | None, current: dict[str, str]) -> dict[str, str] | None:
    if existing is None:
        return None
    for field in ["run", "instance_id", "instance", "status", "incumbent"]:
        if existing.get(field, "") != current.get(field, ""):
            return None
    existing_mtime = existing.get("log_mtime", "")
    current_mtime = current.get("log_mtime", "")
    if existing_mtime and existing_mtime != current_mtime:
        return None
    reused = {field: existing.get(field, "") for field in VALIDATION_FIELDNAMES}
    reused.update(current)
    reused["validation"] = existing.get("validation", "")
    reused["detail"] = existing.get("detail", "")
    return reused


def validate_solutions(
    root: Path,
    checker_jar: Path,
    benchmark_dir: Path,
    workers: int,
    target_slugs: set[str] | None = None,
) -> None:
    if not checker_jar.exists():
        raise SystemExit(f"solution checker jar not found: {checker_jar}")
    instances = dict(read_instances(root))
    all_rows = {run.slug: load_rows(root, run) for run in RUNS}
    previous = load_validation_rows(root)
    validation_dir = logs_root(root) / "validation"
    output_dir = validation_dir / "checker-output"
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_runs = [run for run in RUNS if target_slugs is None or run.slug in target_slugs]
    current_slugs = {run.slug for run in RUNS}

    def validate_one(run: Run, instance_id: int, instance: str) -> dict[str, str]:
        row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
        log_path = log_path_for_row(root, run, instance_id, row_path)
        status = fields[1] if len(fields) > 1 else ""
        current_log_mtime = log_mtime(log_path)
        existing = previous.get((run.slug, instance_id))
        if (
            existing is not None
            and existing.get("log_mtime", "")
            and existing.get("log_mtime", "") == current_log_mtime
            and existing.get("instance", "") == instance
            and existing.get("status", "") == status
        ):
            reused = {field: existing.get(field, "") for field in VALIDATION_FIELDNAMES}
            reused.update(
                {
                    "run": run.slug,
                    "instance_id": str(instance_id),
                    "instance": instance,
                    "status": status,
                    "log_mtime": current_log_mtime,
                }
            )
            return reused
        incumbent, _, _ = parse_log(log_path)
        current = {
            "run": run.slug,
            "instance_id": str(instance_id),
            "instance": instance,
            "status": status,
            "incumbent": incumbent or "",
            "log_mtime": current_log_mtime,
        }
        reused = reusable_validation_row(existing, current)
        if reused is not None:
            return reused
        validation = "skipped_no_incumbent"
        detail = ""
        if status == "unsat":
            validation = "skipped_unsat"
        elif incumbent is not None:
            if log_path is None:
                validation = "no_log"
            else:
                out_path = output_dir / run.slug / f"{instance_id}.txt"
                if out_path.exists() and out_path.stat().st_mtime >= log_path.stat().st_mtime:
                    validation, detail = classify_checker_output(0, out_path.read_text(errors="replace"))
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
        return current | {
            "validation": validation,
            "detail": detail,
        }

    tasks = [(run, instance_id, instance) for run in selected_runs for instance_id, instance in instances.items()]
    rows: list[dict[str, str]] = []
    if target_slugs is not None:
        rows.extend(
            {field: row.get(field, "") for field in VALIDATION_FIELDNAMES}
            for (slug, _), row in previous.items()
            if slug in current_slugs and slug not in target_slugs
        )
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(validate_one, *task) for task in tasks]
        for i, future in enumerate(as_completed(futures), 1):
            rows.append(future.result())
            if i % 500 == 0:
                print(f"validated {i}/{len(futures)} cells", file=sys.stderr)
    rows.sort(key=lambda row: (row["run"], int(row["instance_id"])))
    path = validation_dir / "results.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=VALIDATION_FIELDNAMES)
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
    if validation == "invalid":
        return f"INVALID {label}"
    if validation == "checker_timeout":
        return f"CHECKER_TIMEOUT {label}"
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
    path = logs_root(root) / run.slug / "missing" / f"{instance_id}.log"
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
    all_rows = {run.slug: load_rows(root, run) for run in DOC_RUNS}
    validation = load_validation(root)
    column_labels = [
        COP1000_COLUMN_ALIASES.get(run.slug, run.column[:6]) for run in DOC_RUNS
    ]
    lines = [
        BEGIN,
        "",
        "## COP1000 instance table",
        "",
        "Cell values are incumbent objective values. A trailing `*` means the run proved optimality.",
        "`TO(stage)` and `MO(stage)` mean no incumbent was found before timeout or memory-out at the indicated stage.",
        "Each cell links to the corresponding solver log when available.",
        "`INVALID` marks a solution rejected by validation; `CHECKER_TIMEOUT` means validation did not finish in time; `CHECKER_ERROR` marks a checker failure such as an unsupported solution variable.",
        "",
        "| # | instance | " + " | ".join(md_escape(label) for label in column_labels) + " |",
        "|---:|---|" + "|".join("---:" for _ in DOC_RUNS) + "|",
    ]
    for instance_id, instance in instances:
        row_cells = [str(instance_id), f"`{md_escape(Path(instance).name)}`"]
        for run in DOC_RUNS:
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


def build_result_summary(root: Path) -> str:
    instances = read_instances(root)
    all_rows = {run.slug: load_rows(root, run) for run in DOC_RUNS}
    lines = [
        SUMMARY_BEGIN,
        "",
        "## COP1000 result summary",
        "",
        "`incumbent` counts cells with a reported objective value, including proved optimum cells.",
        "",
        "| run | optimum | incumbent | UNSAT |",
        "|---|---:|---:|---:|",
    ]
    for run in DOC_RUNS:
        optimum = 0
        incumbent = 0
        unsat = 0
        for instance_id, _ in instances:
            row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
            log_path = log_path_for_row(root, run, instance_id, row_path)
            value, _, _ = parse_log(log_path)
            status = fields[1] if len(fields) > 1 else ""
            if status == "optimum":
                optimum += 1
            if value is not None:
                incumbent += 1
            if status == "unsat":
                unsat += 1
        lines.append(f"| `{run.slug}` | {optimum} | {incumbent} | {unsat} |")
    lines.extend(["", SUMMARY_END, ""])
    return "\n".join(lines)


def read_noinc_subset(root: Path) -> list[tuple[int, str]]:
    path = logs_root(root) / NOINC_SUBSET_CSV
    if not path.exists():
        return []
    rows: list[tuple[int, str]] = []
    with path.open(newline="") as f:
        for raw in csv.reader(f):
            if not raw or raw[0] == "instance_id":
                continue
            try:
                instance_id = int(raw[0])
            except ValueError:
                continue
            rows.append((instance_id, raw[-1]))
    rows.sort()
    return rows


def log_reached_sat(log_path: Path | None) -> bool:
    if log_path is None or not log_path.exists():
        return False
    text = log_path.read_text(errors="replace")
    return "stage:sat:solve:start" in text or "stage:sat:init" in text


def family_name(instance: str) -> str:
    name = Path(instance).name
    return name.split("-", 1)[0].split("_", 1)[0]


def parse_elapsed_ms(line: str) -> int | None:
    m = re.match(r"c\s+(\d+)h(\d+)m(\d+)s\s+\|", line)
    if not m:
        return None
    hours, minutes, seconds = (int(part) for part in m.groups())
    return ((hours * 60 + minutes) * 60 + seconds) * 1000


def parse_key_values(line: str) -> dict[str, str]:
    return dict(re.findall(r"([A-Za-z_][A-Za-z0-9_-]*)=([^\s]+)", line))


def read_runsolver_values(values_path: Path | None) -> dict[str, str]:
    if values_path is None or not values_path.exists():
        return {}
    values: dict[str, str] = {}
    for line in values_path.read_text(errors="replace").splitlines():
        if "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def read_noinc_sacct(root: Path) -> dict[str, dict[str, str]]:
    path = logs_root(root) / NOINC_SACCT_CSV
    if not path.exists():
        return {}
    rows: dict[str, dict[str, str]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            job_id = row.get("JobID", "")
            m = re.fullmatch(r"19442938_(\d+)", job_id)
            if m:
                rows[m.group(1)] = row
    return rows


def runsolver_task_id(log_path: Path | None, run: Run) -> str:
    if log_path is None:
        return ""
    escaped = re.escape(run.runsolver_prefix)
    m = re.fullmatch(rf"{escaped}-(\d+)-(\d+)-(\d+)", log_path.parent.name)
    return m.group(2) if m else ""


def parse_reach_sat_details(log_path: Path | None) -> dict[str, str]:
    details: dict[str, str] = {
        "sat_started": "0",
        "sat_rounds": "0",
        "sat_solve_done_rounds": "0",
        "sat_solve_ms_total": "0",
        "first_sat_start_ms": "",
        "last_sat_start_ms": "",
    }
    if log_path is None or not log_path.exists():
        return details

    stage_done_patterns = [
        ("propagate_ms", r"stage:propagate:done ms=(\d+)"),
        ("normalize_ms", r"stage:normalize:done ms=(\d+)"),
        ("lower_ms", r"stage:lower:done ms=(\d+)"),
        ("post_lower_ms", r"stage:post_lower:done ms=(\d+)"),
        ("preencode_ms", r"stage:preencode(?:\([^)]*\))?:done ms=(\d+)"),
        ("encode_ms", r"stage:encode(?:\([^)]*\))?:done ms=(\d+)"),
        ("root_lp_ms", r"stage:root-lp:(?:done|skip).* ms=(\d+)"),
    ]
    latest_stage = ""
    last_completed_stage = ""
    sat_rounds = 0
    sat_done_rounds = 0
    sat_solve_ms_total = 0

    for line in log_path.read_text(errors="replace").splitlines():
        elapsed_ms = parse_elapsed_ms(line)
        if "| stage:" in line:
            m = re.search(r"\|\s+stage:([a-z0-9_-]+)", line)
            if m:
                latest_stage = m.group(1)
        if line.startswith("d CURRENT_STAGE "):
            details["current_stage"] = line.split(None, 2)[2].strip()
        elif line.startswith("d LAST_COMPLETED_STAGE "):
            details["last_completed_stage"] = line.split(None, 2)[2].strip()
        elif line.startswith("d "):
            parts = line.split(None, 2)
            if len(parts) == 3:
                key = parts[1].lower()
                if key in {
                    "source_constraints",
                    "normalized_constraints",
                    "sat_clauses_base",
                    "sat_clauses_final",
                    "sat_clauses_cegar_delta",
                    "circuit_cegar_failures",
                    "circuit_cegar_rounds",
                    "circuit_cegar_added_clauses",
                    "cumulative_cegar_failures",
                    "cumulative_cegar_rounds",
                    "cumulative_cegar_added_clauses",
                    "cop_cegar_failures",
                    "cop_cegar_rounds",
                    "cop_cegar_added_clauses",
                }:
                    details[key] = parts[2].strip()

        for key, pattern in stage_done_patterns:
            m = re.search(pattern, line)
            if m:
                details[key] = m.group(1)
                last_completed_stage = key.removesuffix("_ms")

        if "stage:lowered-shape" in line:
            for key, value in parse_key_values(line).items():
                details[f"lowered_{key}"] = value
        elif "stage:encode" in line and ":done" in line:
            for key, value in parse_key_values(line).items():
                if key in {"sat_vars", "sat_clauses", "ext_ge_specs", "relaxed_circuits"}:
                    details[key] = value
        elif "stage:sat:init" in line:
            details["sat_started"] = "1"
            for key, value in parse_key_values(line).items():
                if key in {"vars", "clauses"}:
                    details[f"sat_init_{key}"] = value
        elif "stage:sat:solve:start" in line:
            details["sat_started"] = "1"
            sat_rounds += 1
            if elapsed_ms is not None:
                if not details.get("first_sat_start_ms"):
                    details["first_sat_start_ms"] = str(elapsed_ms)
                details["last_sat_start_ms"] = str(elapsed_ms)
            values = parse_key_values(line)
            if "round" in values:
                details["last_sat_round"] = values["round"]
        elif "stage:sat:solve:done" in line:
            sat_done_rounds += 1
            values = parse_key_values(line)
            if "ms" in values and values["ms"].isdigit():
                sat_solve_ms_total += int(values["ms"])
            if "result" in values:
                details["last_sat_result"] = values["result"]
        elif "stage:relaxation:done" in line:
            for key, value in parse_key_values(line).items():
                if key.endswith("_clauses_added") or key.endswith("_cegar_failures") or key.endswith("_cegar_rounds") or key.endswith("_cegar_added_clauses"):
                    details[key] = value

    details["sat_rounds"] = str(sat_rounds)
    details["sat_solve_done_rounds"] = str(sat_done_rounds)
    details["sat_solve_ms_total"] = str(sat_solve_ms_total)
    details.setdefault("current_stage", latest_stage)
    details.setdefault("last_completed_stage", last_completed_stage)
    return details


def failure_class(
    row_path: Path | None,
    fields: list[str],
    incumbent: str | None,
    sat_started: bool,
    log_outcome: str | None,
    values: dict[str, str],
    slurm_state: str = "",
) -> str:
    status = fields[1] if len(fields) > 1 else ""
    reason, _ = row_reason(fields)
    if incumbent is not None:
        return "incumbent"
    if status in {"optimum", "sat", "unsat"}:
        return "solved_no_incumbent"
    if reason == "lower_unsupported_constraints":
        return "unsupported_before_sat"
    if reason == "encode_failed":
        return "encode_failed_before_sat"
    memout = (
        values.get("MEMOUT", "").lower() == "true"
        or log_outcome == "MO"
        or slurm_state == "OUT_OF_MEMORY"
    )
    timeout = values.get("TIMEOUT", "").lower() == "true" or log_outcome == "TO"
    if memout:
        return "oom_after_sat" if sat_started else "oom_before_sat"
    if timeout:
        return "timeout_after_sat" if sat_started else "timeout_before_sat"
    if row_path is None:
        return "missing_row"
    if sat_started:
        return "sat_reached_no_incumbent"
    if not fields:
        return "empty_row"
    return "stopped_before_sat"


DIAGNOSIS_FIELDNAMES = [
    "instance_id",
    "instance",
    "family",
    "row_present",
    "row_nonempty",
    "log_present",
    "values_present",
    "status",
    "reason",
    "detail",
    "incumbent",
    "validation",
    "sat_started",
    "failure_class",
    "current_stage",
    "last_completed_stage",
    "log_outcome",
    "wctime",
    "cputime",
    "maxrss_kib",
    "maxmm_kib",
    "maxvm_kib",
    "timeout_flag",
    "memout_flag",
    "exitstatus",
    "slurm_task_id",
    "slurm_state",
    "slurm_exit_code",
    "slurm_elapsed",
    "slurm_maxrss",
    "first_sat_start_ms",
    "last_sat_start_ms",
    "sat_rounds",
    "sat_solve_done_rounds",
    "sat_solve_ms_total",
    "source_constraints",
    "normalized_constraints",
    "propagate_ms",
    "normalize_ms",
    "lower_ms",
    "post_lower_ms",
    "root_lp_ms",
    "preencode_ms",
    "encode_ms",
    "sat_vars",
    "sat_clauses",
    "sat_init_vars",
    "sat_init_clauses",
    "ext_ge_specs",
    "relaxed_circuits",
    "lowered_int_vars",
    "lowered_bool_vars",
    "lowered_linear_atoms",
    "lowered_table_atoms",
    "lowered_table_tuples",
    "lowered_alldiff_atoms",
    "lowered_circuit_atoms",
    "lowered_logical_clauses",
    "circuit_cegar_failures",
    "circuit_cegar_rounds",
    "circuit_cegar_added_clauses",
    "cumulative_cegar_failures",
    "cumulative_cegar_rounds",
    "cumulative_cegar_added_clauses",
    "cop_cegar_failures",
    "cop_cegar_rounds",
    "cop_cegar_added_clauses",
    "baseline_status",
    "baseline_sat_started",
    "baseline_stage",
    "baseline_outcome",
    "log_path",
    "values_path",
]


def write_noinc_subset_diagnosis(root: Path) -> None:
    subset = read_noinc_subset(root)
    runs = {run.slug: run for run in RUNS}
    if NOINC_TARGET_RUN not in runs or NOINC_BASELINE_RUN not in runs:
        return
    target = runs[NOINC_TARGET_RUN]
    baseline = runs[NOINC_BASELINE_RUN]
    target_rows = load_rows(root, target)
    baseline_rows = load_rows(root, baseline)
    validation = load_validation(root)
    sacct = read_noinc_sacct(root)
    rows: list[dict[str, str]] = []

    for instance_id, instance in subset:
        row_path, fields = target_rows.get(instance_id, (None, []))
        log_path = log_path_for_row(root, target, instance_id, row_path)
        values_path = runsolver_values_path(log_path) if log_path is not None else None
        values = read_runsolver_values(values_path)
        incumbent, stage, log_outcome = parse_log(log_path)
        details = parse_reach_sat_details(log_path)
        reason, detail = row_reason(fields)
        slurm_task_id = runsolver_task_id(log_path, target)
        slurm = sacct.get(slurm_task_id, {})
        baseline_row_path, baseline_fields = baseline_rows.get(instance_id, (None, []))
        baseline_log_path = log_path_for_row(root, baseline, instance_id, baseline_row_path)
        _, baseline_stage, baseline_outcome = parse_log(baseline_log_path)
        sat_started = details.get("sat_started", "0") == "1"

        row = {
            "instance_id": str(instance_id),
            "instance": instance,
            "family": family_name(instance),
            "row_present": "1" if row_path is not None else "0",
            "row_nonempty": "1" if fields else "0",
            "log_present": "1" if log_path is not None else "0",
            "values_present": "1" if values_path is not None else "0",
            "status": fields[1] if len(fields) > 1 else "",
            "reason": reason,
            "detail": detail,
            "incumbent": incumbent or "",
            "validation": validation.get((target.slug, instance_id), ""),
            "sat_started": "1" if sat_started else "0",
            "failure_class": failure_class(row_path, fields, incumbent, sat_started, log_outcome, values),
            "current_stage": details.get("current_stage") or stage or "",
            "last_completed_stage": details.get("last_completed_stage", ""),
            "log_outcome": log_outcome or "",
            "wctime": values.get("WCTIME", ""),
            "cputime": values.get("CPUTIME", ""),
            "maxrss_kib": values.get("MAXRSS", ""),
            "maxmm_kib": values.get("MAXMM", ""),
            "maxvm_kib": values.get("MAXVM", ""),
            "timeout_flag": values.get("TIMEOUT", ""),
            "memout_flag": values.get("MEMOUT", ""),
            "exitstatus": values.get("EXITSTATUS", ""),
            "slurm_task_id": slurm_task_id,
            "slurm_state": slurm.get("State", ""),
            "slurm_exit_code": slurm.get("ExitCode", ""),
            "slurm_elapsed": slurm.get("Elapsed", ""),
            "slurm_maxrss": slurm.get("MaxRSS", ""),
            "baseline_status": baseline_fields[1] if len(baseline_fields) > 1 else "",
            "baseline_sat_started": "1" if log_reached_sat(baseline_log_path) else "0",
            "baseline_stage": baseline_stage or "",
            "baseline_outcome": baseline_outcome or "",
            "log_path": log_path.relative_to(root).as_posix() if log_path is not None else "",
            "values_path": values_path.relative_to(root).as_posix() if values_path is not None else "",
        }
        row["failure_class"] = failure_class(
            row_path,
            fields,
            incumbent,
            sat_started,
            log_outcome,
            values,
            slurm.get("State", ""),
        )
        for field in DIAGNOSIS_FIELDNAMES:
            row.setdefault(field, details.get(field, ""))
        rows.append(row)

    path = logs_root(root) / NOINC_DIAGNOSIS_CSV
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DIAGNOSIS_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def build_noinc_subset_summary(root: Path) -> str:
    subset = read_noinc_subset(root)
    runs = {run.slug: run for run in RUNS}
    selected_runs = [runs[slug] for slug in NOINC_SUBSET_RUNS if slug in runs]
    all_rows = {run.slug: load_rows(root, run) for run in selected_runs}
    lines = [
        NOINC_SUBSET_BEGIN,
        "",
        "## COP1000 no-incumbent subset summary",
        "",
        f"This table is restricted to the `{NOINC_SUBSET_CSV}` subset ({len(subset)} instances):",
        "a precomputed set of instances with no incumbent in the subset baseline.",
        "`sat_started` counts rows whose log reached the SAT solve stage, even if no incumbent was found.",
        "",
        "| run | row_files | nonempty_csv | incumbent | sat_started | optimum | sat | UNSAT | TO | MO | missing_file |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for run in selected_runs:
        counts: Counter[str] = Counter()
        for instance_id, _ in subset:
            row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
            log_path = log_path_for_row(root, run, instance_id, row_path)
            incumbent, _, log_outcome = parse_log(log_path)
            status = fields[1] if len(fields) > 1 else ""
            if row_path is not None:
                counts["row_files"] += 1
            if fields:
                counts["nonempty_csv"] += 1
            if row_path is None:
                counts["missing_file"] += 1
            if incumbent is not None:
                counts["incumbent"] += 1
            if log_reached_sat(log_path):
                counts["sat_started"] += 1
            if status == "optimum":
                counts["optimum"] += 1
            if status == "sat":
                counts["sat"] += 1
            if status == "unsat":
                counts["unsat"] += 1
            label = classify_cell(fields, incumbent, None, log_path is not None, log_outcome)
            if label.startswith("TO("):
                counts["to"] += 1
            if label.startswith("MO("):
                counts["mo"] += 1
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{run.slug}`",
                    str(counts["row_files"]),
                    str(counts["nonempty_csv"]),
                    str(counts["incumbent"]),
                    str(counts["sat_started"]),
                    str(counts["optimum"]),
                    str(counts["sat"]),
                    str(counts["unsat"]),
                    str(counts["to"]),
                    str(counts["mo"]),
                    str(counts["missing_file"]),
                ]
            )
            + " |"
        )
    lines.extend(["", NOINC_SUBSET_END, ""])
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
    for run in DOC_RUNS:
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


def parse_decimal(text: str | None) -> Decimal | None:
    if text is None:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def is_better(value: Decimal, incumbent: Decimal, sense: str) -> bool:
    if sense == "minimize":
        return value < incumbent
    if sense == "maximize":
        return value > incumbent
    return False


def format_decimal(value: object) -> str:
    if not isinstance(value, Decimal):
        return str(value)
    if value == value.to_integral_value():
        return str(value.quantize(Decimal(1)))
    return format(value.normalize(), "f")


def consistency_issues(root: Path, benchmark_dir: Path) -> list[dict[str, str]]:
    instances = read_instances(root)
    runs = {run.slug: run for run in RUNS}
    selected_runs = [runs[slug] for slug in CONSISTENCY_RUNS if slug in runs]
    all_rows = {run.slug: load_rows(root, run) for run in selected_runs}
    validation = load_validation(root)
    issues: list[dict[str, str]] = []

    for instance_id, instance in instances:
        sense = objective_sense(benchmark_dir, instance)
        entries: list[dict[str, object]] = []
        for run in selected_runs:
            row_path, fields = all_rows[run.slug].get(instance_id, (None, []))
            log_path = log_path_for_row(root, run, instance_id, row_path)
            incumbent, _, _ = parse_log(log_path)
            status = fields[1] if len(fields) > 1 else ""
            value = parse_decimal(incumbent)
            check = validation.get((run.slug, instance_id), "")
            if check in BAD_VALIDATION:
                continue
            entries.append(
                {
                    "run": run.slug,
                    "status": status,
                    "value": value,
                }
            )

        unsat_runs = [entry["run"] for entry in entries if entry["status"] == "unsat"]
        value_entries = [entry for entry in entries if entry["value"] is not None]
        if unsat_runs and value_entries:
            issues.append(
                {
                    "instance_id": str(instance_id),
                    "instance": instance,
                    "sense": sense,
                    "issue": "unsat_with_value",
                    "detail": "UNSAT from "
                    + ", ".join(str(run) for run in unsat_runs)
                    + "; values from "
                    + ", ".join(
                        f"{entry['run']}={format_decimal(entry['value'])}" for entry in value_entries[:8]
                    ),
                }
            )

        opt_entries = [
            entry for entry in entries if entry["status"] == "optimum" and entry["value"] is not None
        ]
        opt_values = {entry["value"] for entry in opt_entries}
        if len(opt_values) > 1:
            issues.append(
                {
                    "instance_id": str(instance_id),
                    "instance": instance,
                    "sense": sense,
                    "issue": "optimal_mismatch",
                    "detail": "; ".join(
                        f"{entry['run']}={format_decimal(entry['value'])}" for entry in opt_entries
                    ),
                }
            )

        if len(opt_values) == 1 and sense in {"minimize", "maximize"}:
            optimum = next(iter(opt_values))
            better_entries = [
                entry
                for entry in value_entries
                if entry["status"] != "optimum"
                and isinstance(entry["value"], Decimal)
                and is_better(entry["value"], optimum, sense)
            ]
            if better_entries:
                issues.append(
                    {
                        "instance_id": str(instance_id),
                        "instance": instance,
                        "sense": sense,
                        "issue": "incumbent_beats_optimum",
                        "detail": f"optimum={format_decimal(optimum)}; "
                        + "; ".join(
                            f"{entry['run']}={format_decimal(entry['value'])}" for entry in better_entries
                        ),
                    }
                )
    return issues


def write_consistency_results(root: Path, benchmark_dir: Path) -> None:
    rows = consistency_issues(root, benchmark_dir)
    path = logs_root(root) / "consistency" / "results.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        fieldnames = ["instance_id", "instance", "sense", "issue", "detail"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_consistency(root: Path) -> list[dict[str, str]]:
    path = logs_root(root) / "consistency" / "results.csv"
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_consistency_stats(root: Path) -> str:
    rows = load_consistency(root)
    counts = Counter(row.get("issue", "") for row in rows)
    lines = [
        CONSISTENCY_BEGIN,
        "",
        "## Cross-solver consistency stats",
        "",
        "This check compares only the configured visible runs for each instance after excluding cells marked `invalid`, `checker_error`, or `checker_timeout` by validation.",
        "It reports differing proved optima, incumbents that beat a proved optimum according to the XCSP3 objective sense, and UNSAT/value contradictions.",
        "",
        "| issue | count |",
        "|---|---:|",
    ]
    for issue in ["optimal_mismatch", "incumbent_beats_optimum", "unsat_with_value"]:
        lines.append(f"| `{issue}` | {counts[issue]} |")
    if rows:
        lines.extend(
            [
                "",
                "Detailed results are in `docs/logs/consistency/results.csv`. Issues shown below are capped at 50 rows:",
                "",
                "| # | instance | sense | issue | detail |",
                "|---:|---|---|---|---|",
            ]
        )
        for row in rows[:50]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        row["instance_id"],
                        f"`{md_escape(Path(row['instance']).name)}`",
                        md_escape(row["sense"]),
                        f"`{md_escape(row['issue'])}`",
                        md_escape(row["detail"]),
                    ]
                )
                + " |"
            )
    else:
        lines.extend(["", "No cross-solver inconsistencies were found."])
    lines.extend(["", CONSISTENCY_END, ""])
    return "\n".join(lines)


def comparison_cell(
    root: Path,
    run: Run,
    rows: dict[int, tuple[Path, list[str]]],
    validation: dict[tuple[str, int], str],
    instance_id: int,
) -> dict[str, object]:
    row_path, fields = rows.get(instance_id, (None, []))
    log_path = log_path_for_row(root, run, instance_id, row_path)
    incumbent, _, _ = parse_log(log_path)
    return {
        "status": fields[1] if len(fields) > 1 else "",
        "value": parse_decimal(incumbent),
        "validation": validation.get((run.slug, instance_id), ""),
    }


def cell_value_text(cell: dict[str, object]) -> str:
    status = str(cell.get("status", ""))
    value = cell.get("value")
    value_text = format_decimal(value) if isinstance(value, Decimal) else "NA"
    return f"{status or 'unknown'}:{value_text}"


def reference_comparison(
    root: Path, benchmark_dir: Path
) -> tuple[list[dict[str, int | str]], list[dict[str, str]]]:
    instances = read_instances(root)
    runs = {run.slug: run for run in RUNS}
    target = runs[REFERENCE_COMPARE_TARGET]
    references = [runs[slug] for slug in REFERENCE_COMPARE_RUNS]
    selected = [target] + references
    all_rows = {run.slug: load_rows(root, run) for run in selected}
    validation = load_validation(root)
    stats: list[dict[str, int | str]] = []
    issues: list[dict[str, str]] = []

    for reference in references:
        counts: Counter[str] = Counter()
        for instance_id, instance in instances:
            target_cell = comparison_cell(
                root,
                target,
                all_rows[target.slug],
                validation,
                instance_id,
            )
            reference_cell = comparison_cell(
                root,
                reference,
                all_rows[reference.slug],
                validation,
                instance_id,
            )
            if (
                target_cell["validation"] in BAD_VALIDATION
                or reference_cell["validation"] in BAD_VALIDATION
            ):
                counts["excluded_by_validation"] += 1
                continue

            counts["comparable"] += 1
            target_value = target_cell["value"]
            reference_value = reference_cell["value"]
            target_status = str(target_cell["status"])
            reference_status = str(reference_cell["status"])
            target_has_value = isinstance(target_value, Decimal)
            reference_has_value = isinstance(reference_value, Decimal)

            if target_has_value:
                counts["target_incumbent"] += 1
            if reference_has_value:
                counts["reference_incumbent"] += 1
            if target_has_value and reference_has_value:
                counts["both_incumbent"] += 1
                if target_value == reference_value:
                    counts["same_value"] += 1
                else:
                    pass

            sense = objective_sense(benchmark_dir, instance)
            issue = ""
            if (target_status == "unsat" and reference_has_value) or (
                reference_status == "unsat" and target_has_value
            ):
                issue = "unsat_with_value"
            elif (
                target_status == "optimum"
                and reference_status == "optimum"
                and target_has_value
                and reference_has_value
                and target_value != reference_value
            ):
                issue = "proved_optimum_mismatch"
            elif (
                target_status == "optimum"
                and target_has_value
                and reference_has_value
                and is_better(reference_value, target_value, sense)
            ):
                issue = "reference_beats_target_optimum"
            elif (
                reference_status == "optimum"
                and target_has_value
                and reference_has_value
                and is_better(target_value, reference_value, sense)
            ):
                issue = "target_beats_reference_optimum"
            if issue:
                counts[issue] += 1
                issues.append(
                    {
                        "reference_run": reference.slug,
                        "instance_id": str(instance_id),
                        "instance": instance,
                        "sense": sense,
                        "issue": issue,
                        "target": cell_value_text(target_cell),
                        "reference": cell_value_text(reference_cell),
                    }
                )

        stats.append(
            {
                "reference_run": reference.slug,
                "comparable": counts["comparable"],
                "target_incumbent": counts["target_incumbent"],
                "reference_incumbent": counts["reference_incumbent"],
                "both_incumbent": counts["both_incumbent"],
                "same_value": counts["same_value"],
                "proved_optimum_mismatch": counts["proved_optimum_mismatch"],
                "incumbent_beats_proved_optimum": counts["reference_beats_target_optimum"]
                + counts["target_beats_reference_optimum"],
                "unsat_with_value": counts["unsat_with_value"],
                "excluded_by_validation": counts["excluded_by_validation"],
            }
        )

    return stats, issues


def write_reference_comparison_results(root: Path, benchmark_dir: Path) -> None:
    _, rows = reference_comparison(root, benchmark_dir)
    path = logs_root(root) / "reference-comparison" / "results.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        fieldnames = [
            "reference_run",
            "instance_id",
            "instance",
            "sense",
            "issue",
            "target",
            "reference",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_reference_comparison_stats(root: Path, benchmark_dir: Path) -> str:
    stats, issues = reference_comparison(root, benchmark_dir)
    lines = [
        REFERENCE_COMPARE_BEGIN,
        "",
        "## Reference solver comparison",
        "",
        f"This compares `{REFERENCE_COMPARE_TARGET}` with ACE and OR-Tools reference runs after excluding cells marked `invalid`, `checker_error`, or `checker_timeout` by validation.",
        "Plain incumbent/value mismatches are intentionally ignored here; only proved-optimum contradictions and UNSAT/value contradictions are reported.",
        "",
        "| reference run | comparable | target incumbent | reference incumbent | both incumbent | same value | proved_optimum_mismatch | incumbent_beats_proved_optimum | unsat_with_value | excluded_by_validation |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in stats:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row['reference_run']}`",
                    str(row["comparable"]),
                    str(row["target_incumbent"]),
                    str(row["reference_incumbent"]),
                    str(row["both_incumbent"]),
                    str(row["same_value"]),
                    str(row["proved_optimum_mismatch"]),
                    str(row["incumbent_beats_proved_optimum"]),
                    str(row["unsat_with_value"]),
                    str(row["excluded_by_validation"]),
                ]
            )
            + " |"
        )

    if issues:
        lines.extend(
            [
                "",
                "Detailed mismatch rows are in `docs/logs/reference-comparison/results.csv`. Rows shown below are capped at 50:",
                "",
                "| reference run | # | instance | sense | issue | target | reference |",
                "|---|---:|---|---|---|---|---|",
            ]
        )
        for row in issues[:50]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{md_escape(row['reference_run'])}`",
                        row["instance_id"],
                        f"`{md_escape(Path(row['instance']).name)}`",
                        md_escape(row["sense"]),
                        f"`{md_escape(row['issue'])}`",
                        f"`{md_escape(row['target'])}`",
                        f"`{md_escape(row['reference'])}`",
                    ]
                )
                + " |"
            )
    else:
        lines.extend(["", "No mismatches were found for the configured reference comparisons."])

    lines.extend(["", REFERENCE_COMPARE_END, ""])
    return "\n".join(lines)


def update_doc(root: Path, benchmark_dir: Path) -> None:
    doc = root / "cop-results.md"
    table = build_table(root)
    result_summary = build_result_summary(root)
    noinc_subset_summary = build_noinc_subset_summary(root)
    validation_stats = build_validation_stats(root)
    consistency_stats = build_consistency_stats(root)
    reference_comparison_stats = build_reference_comparison_stats(root, benchmark_dir)
    if doc.exists():
        text = doc.read_text()
    else:
        text = "# COP Results\n\n"
    if SUMMARY_BEGIN in text and SUMMARY_END in text:
        pre = text.split(SUMMARY_BEGIN, 1)[0].rstrip()
        post = text.split(SUMMARY_END, 1)[1].lstrip()
        text = pre + "\n\n" + result_summary + ("\n" + post if post else "")
    else:
        marker = VALIDATION_BEGIN if VALIDATION_BEGIN in text else (BEGIN if BEGIN in text else None)
        if marker:
            pre, post = text.split(marker, 1)
            text = pre.rstrip() + "\n\n" + result_summary + "\n" + marker + post
        else:
            text = text.rstrip() + "\n\n" + result_summary
    if NOINC_SUBSET_BEGIN in text and NOINC_SUBSET_END in text:
        pre = text.split(NOINC_SUBSET_BEGIN, 1)[0].rstrip()
        post = text.split(NOINC_SUBSET_END, 1)[1].lstrip()
        text = pre + "\n\n" + noinc_subset_summary + ("\n" + post if post else "")
    else:
        marker = VALIDATION_BEGIN if VALIDATION_BEGIN in text else (BEGIN if BEGIN in text else None)
        if marker:
            pre, post = text.split(marker, 1)
            text = pre.rstrip() + "\n\n" + noinc_subset_summary + "\n" + marker + post
        else:
            text = text.rstrip() + "\n\n" + noinc_subset_summary
    if CONSISTENCY_BEGIN in text and CONSISTENCY_END in text:
        pre = text.split(CONSISTENCY_BEGIN, 1)[0].rstrip()
        post = text.split(CONSISTENCY_END, 1)[1].lstrip()
        text = pre + "\n\n" + consistency_stats + ("\n" + post if post else "")
    else:
        marker = BEGIN if BEGIN in text else None
        if marker:
            pre, post = text.split(marker, 1)
            text = pre.rstrip() + "\n\n" + consistency_stats + "\n" + marker + post
        else:
            text = text.rstrip() + "\n\n" + consistency_stats
    if REFERENCE_COMPARE_BEGIN in text and REFERENCE_COMPARE_END in text:
        pre = text.split(REFERENCE_COMPARE_BEGIN, 1)[0].rstrip()
        post = text.split(REFERENCE_COMPARE_END, 1)[1].lstrip()
        text = pre + "\n\n" + reference_comparison_stats + ("\n" + post if post else "")
    else:
        marker = BEGIN if BEGIN in text else None
        if marker:
            pre, post = text.split(marker, 1)
            text = pre.rstrip() + "\n\n" + reference_comparison_stats + "\n" + marker + post
        else:
            text = text.rstrip() + "\n\n" + reference_comparison_stats
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
    parser.add_argument(
        "--validate-run",
        action="append",
        default=[],
        metavar="SLUG",
        help="Validate only the given run slug and keep existing validation rows for other runs",
    )
    parser.add_argument("--checker-jar", type=Path, default=DEFAULT_CHECKER_JAR)
    parser.add_argument("--benchmark-dir", type=Path, default=DEFAULT_BENCHMARK_DIR)
    parser.add_argument("--validation-workers", type=int, default=4)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not args.no_fetch:
        fetch_logs(root)
    validation_targets = set(args.validate_run) or None
    if validation_targets is not None:
        known_slugs = {run.slug for run in RUNS}
        unknown = sorted(validation_targets - known_slugs)
        if unknown:
            raise SystemExit(f"unknown --validate-run slug(s): {', '.join(unknown)}")
    if args.validate or validation_targets is not None:
        validate_solutions(
            root,
            args.checker_jar,
            args.benchmark_dir,
            args.validation_workers,
            validation_targets,
        )
    write_noinc_subset_diagnosis(root)
    write_consistency_results(root, args.benchmark_dir)
    write_reference_comparison_results(root, args.benchmark_dir)
    update_doc(root, args.benchmark_dir)


if __name__ == "__main__":
    main()
