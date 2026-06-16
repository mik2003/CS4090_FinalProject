#!/usr/bin/env bash
N="${1:?Usage: $0 <number_of_nodes>}"

NODES=$(for i in $(seq 0 $((N-1))); do printf "Node%d," "$i"; done | sed 's/,$//')

if [ ! -f ~/.simulaqron_pids/simulaqron_network_default.pid ]; then
    simulaqron start --nodes="$NODES" \
        --network-config-file simulaqron_network.json \
        --simulaqron-config-file simulaqron_settings.json
    echo "Started Simulaqron with nodes: $NODES"
else
    echo "Simulaqron is already running. Please stop it before starting a new instance."
fi