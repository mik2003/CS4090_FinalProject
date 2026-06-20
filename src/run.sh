#!/usr/bin/env bash
# run.sh — clean restart of SimulaQron + the four anonymous-QKD nodes.
# Usage: ./run.sh          (Ctrl-C tears everything back down)
#
# Knobs:
#   SIM_WAIT=10 ./run.sh   # wait longer for SimulaQron to boot on a slow machine

N=4
SIM_WAIT="${SIM_WAIT:-6}"        # seconds to let SimulaQron boot before launching nodes
LOG_DIR="logs"

# Run from the directory this script lives in, so relative paths work from anywhere.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" || exit 1

PIDS=()

cleanup() {
    echo
    echo "==> Tearing down..."
    [ "${#PIDS[@]}" -gt 0 ] && kill -9 "${PIDS[@]}" 2>/dev/null
    pkill -9 -f anonymous_qkd.py 2>/dev/null
    ./terminate.sh
    exit 0
}
trap cleanup INT TERM

echo "==> Stopping any previous run..."
./terminate.sh
sleep 2

echo "==> Starting SimulaQron with $N nodes..."
./start.sh "$N"

echo "==> Waiting ${SIM_WAIT}s for SimulaQron to come up..."
sleep "$SIM_WAIT"

mkdir -p "$LOG_DIR"

echo "==> Launching nodes..."
# args: <index> <role: S|R|N> [LAST]
python anonymous_qkd.py 0 S      > "$LOG_DIR/node0.log" 2>&1 &  PIDS+=($!)
python anonymous_qkd.py 1 N      > "$LOG_DIR/node1.log" 2>&1 &  PIDS+=($!)
python anonymous_qkd.py 2 R      > "$LOG_DIR/node2.log" 2>&1 &  PIDS+=($!)
python anonymous_qkd.py 3 N LAST > "$LOG_DIR/node3.log" 2>&1 &  PIDS+=($!)

echo "    Node0 (S)       -> $LOG_DIR/node0.log"
echo "    Node1 (N)       -> $LOG_DIR/node1.log"
echo "    Node2 (R)       -> $LOG_DIR/node2.log"
echo "    Node3 (N, LAST) -> $LOG_DIR/node3.log"
echo
echo "==> Streaming logs. Ctrl-C stops everything cleanly."
echo
tail -f "$LOG_DIR"/node*.log