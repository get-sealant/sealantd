#!/usr/bin/env bash
# End-to-end sealantd demo (plan §22 Phase 8): build, start the daemon, drive a command through it
# over the Unix control socket via sealantctl, observe telemetry, then shut down gracefully.
#
#   examples/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> building sealantd + sealantctl"
cargo build -q -p sealantd -p sealantctl

WS="$(mktemp -d)"
SOCK="$(mktemp -u).sock"
echo ">> workspace: $WS"
echo ">> starting daemon (filesystem watch + egress proxy enabled)"
./target/debug/sealantd --socket "$SOCK" --workspace "$WS" \
  --watch-filesystem --network-proxy --log-level off &
DAEMON=$!
trap 'kill "$DAEMON" 2>/dev/null || true; rm -rf "$WS" "$SOCK"' EXIT
for _ in $(seq 1 100); do [ -S "$SOCK" ] && break; sleep 0.05; done

echo; echo ">> health";       ./target/debug/sealantctl --socket "$SOCK" health
echo; echo ">> capabilities"; ./target/debug/sealantctl --socket "$SOCK" capabilities
echo; echo ">> exec (stream telemetry until exit): write a file and print it"
./target/debug/sealantctl --socket "$SOCK" exec --wait /bin/sh -- \
  -c 'echo hello-from-sealant > note.txt && cat note.txt'
echo; echo ">> graceful shutdown (emits the baseline->final filesystem diff)"
./target/debug/sealantctl --socket "$SOCK" shutdown --grace 300
wait "$DAEMON" 2>/dev/null || true
echo ">> demo complete"
