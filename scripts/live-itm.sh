#!/usr/bin/env bash
#
# Launch live-itm inside a detached tmux session so the TUI keeps running on
# this host and can be attached from another machine over (Tailscale) SSH,
# surviving disconnects.
#
# Usage:
#   scripts/live-itm.sh [start|attach|stop|status] [-- <live-itm args...>]
#
# Examples:
#   scripts/live-itm.sh                      # start (default action)
#   scripts/live-itm.sh start -- --min-ask 0.96 --max-ask 0.99
#   scripts/live-itm.sh attach               # attach the TUI on this host
#   scripts/live-itm.sh stop
#
# Attach from another machine over SSH (note the -t to force a PTY, which the
# ratatui TUI needs to render):
#   ssh -t <this-host> 'tmux attach -t live-itm'

set -euo pipefail

SESSION="live-itm"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

action="${1:-start}"
if [ "$#" -gt 0 ]; then shift; fi
# Allow an optional `--` separator before the live-itm args.
if [ "${1:-}" = "--" ]; then shift; fi
# Remaining "$@" are forwarded verbatim to live-itm.

require_tmux() {
  command -v tmux >/dev/null 2>&1 || {
    echo "tmux not found on this host; install it (e.g. apt install tmux)." >&2
    exit 1
  }
}

session_exists() {
  tmux has-session -t "$SESSION" 2>/dev/null
}

case "$action" in
  start)
    require_tmux
    if session_exists; then
      echo "Session '$SESSION' is already running. Attach with:  $0 attach"
      exit 0
    fi
    # Build here (outside tmux) so compile errors surface in this shell and the
    # tmux session runs the prebuilt binary without needing cargo on its PATH.
    ( cd "$REPO_ROOT" && cargo build --release -p live-itm )
    BIN="$REPO_ROOT/target/release/live-itm"
    # Safely quote the binary + any forwarded args into one command string.
    printf -v CMD '%q ' "$BIN" "$@"
    tmux new-session -d -s "$SESSION" -c "$REPO_ROOT" "$CMD"
    echo "Started '$SESSION'."
    echo "Attach on this host:       $0 attach"
    echo "Attach from another host:  ssh -t \$(hostname) 'tmux attach -t $SESSION'"
    ;;
  attach)
    require_tmux
    session_exists || { echo "No session '$SESSION'. Start it with:  $0 start" >&2; exit 1; }
    exec tmux attach -t "$SESSION"
    ;;
  stop)
    require_tmux
    if session_exists; then
      tmux kill-session -t "$SESSION"
      echo "Stopped '$SESSION'."
    else
      echo "No session '$SESSION' to stop."
    fi
    ;;
  status)
    require_tmux
    if session_exists; then echo "running"; else echo "stopped"; fi
    ;;
  *)
    echo "Usage: $0 [start|attach|stop|status] [-- <live-itm args...>]" >&2
    exit 1
    ;;
esac
