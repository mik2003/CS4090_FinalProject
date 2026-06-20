#!/usr/bin/env sh
# Kill any running anonymous-QKD node processes
pkill -9 -f anonymous_qkd.py 2>/dev/null

# Stop SimulaQron and clear its pid file (a stale file blocks start.sh)
PIDFILE="$HOME/.simulaqron_pids/simulaqron_network_default.pid"
if [ -f "$PIDFILE" ]; then
    if ! simulaqron stop; then
        xargs kill -9 < "$PIDFILE" 2>/dev/null
    fi
    rm -f "$PIDFILE"
fi