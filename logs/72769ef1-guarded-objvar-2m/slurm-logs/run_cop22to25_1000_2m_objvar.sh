#!/usr/bin/env bash
#SBATCH --job-name=kat-cop-objvar-2m
#SBATCH --partition=gr10672b
#SBATCH --array=1-1000%8
#SBATCH --time=00:04:00
#SBATCH --rsc=p=1:t=16:c=16:m=60000M
#SBATCH --output=/LARGE0/gr10672/b39275/xcsp3instances/slurm-logs/kat-cop22to25-1000-2m-guarded-basic-objvar-20260429-72769ef1-dirty/slurm-%A-%a.out
#SBATCH --error=/LARGE0/gr10672/b39275/xcsp3instances/slurm-logs/kat-cop22to25-1000-2m-guarded-basic-objvar-20260429-72769ef1-dirty/slurm-%A-%a.err

set -u
BASE=/LARGE0/gr10672/b39275/xcsp3instances
LIST="$BASE/instance-lists/cop22to25_1000.csv"
BIN="$BASE/bin-overrides/kat_xcsp_72769ef1_dirty_objvar_20260429"
RESULT="$BASE/results/kat-cop22to25-1000-2m-guarded-basic-objvar-20260429-72769ef1-dirty"
TASK_ID="${SLURM_ARRAY_TASK_ID}"
LINE="$(python3 - "$LIST" "$TASK_ID" <<'PY2'
import csv, sys
path, task = sys.argv[1], int(sys.argv[2])
with open(path, newline='') as f:
    for row in csv.DictReader(f):
        if int(row['instance_id']) == task:
            print(row['instance_id'] + ',' + row['instance_relpath'])
            break
PY2
)"
INSTANCE_ID="${LINE%%,*}"
REL="${LINE#*,}"
INSTANCE="$BASE/benchmark/$REL"
OUT="$RESULT/out/run-${SLURM_ARRAY_JOB_ID}-${TASK_ID}.out"
ROW="$RESULT/rows/row-${TASK_ID}.csv"
export KAT_XCSP_PARSER=rust
export OMP_NUM_THREADS=1
START_MS=$(date +%s%3N)

timeout -s TERM -k 15s 120s "$BIN" "$INSTANCE"   --encoder order-ge   --order-ge-table mdd-tl   --order-ge-eq-ne direct-order   --order-ge-direct-eq-ne-max-arity 2   --cop-pipeline guarded-basic   --progress   > "$OUT" 2>&1
RC=$?
END_MS=$(date +%s%3N)
python3 - "$OUT" "$ROW" "$INSTANCE" "$RC" "$START_MS" "$END_MS" <<'PY3'
import csv, sys
out_path, row_path, inst, rc, start_ms, end_ms = sys.argv[1:]
text = open(out_path, errors='replace').read().splitlines()
rows = [line[6:] for line in text if line.startswith('d CSV ')]
if rows:
    open(row_path, 'w').write(rows[-1] + '
')
else:
    elapsed = str(max(0, int(end_ms) - int(start_ms)))
    row = [inst, 'unknown', '', '', '', '', '', '', elapsed, '', '', '', '', '', '', 'runner_timeout' if rc in ('124','137','143') else 'runner_error', 'no d CSV row emitted; exit=' + rc]
    with open(row_path, 'w', newline='') as f:
        csv.writer(f).writerow(row)
PY3
exit 0
