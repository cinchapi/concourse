#!/usr/bin/env bash

# Copyright (c) 2013-2025 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Normalize working directory, load shared functions, etc.
. "$(dirname "$0")/.include"

# Exit immediately if a command exits with a non-zero status,
# and treat unset variables as an error.
set -euo pipefail

###############################################################################
# Usage information
###############################################################################
usage() {
    echo "Usage: $0 --nodes <port[,port,...]> [--nodes <port>] [--rf <replication_factor>] [--clean]"
    echo "  --nodes, --node, -n    Specify one or more ports on which the nodes will run."
    echo "                         You can provide a comma-separated list or repeat the flag."
    echo "  --rf                   Optional: Set the replication factor. Default: (#nodes/2)+1"
    echo "  --clean                Force generation of a new installer even if one exists."
    echo "  -h, --help             Display this help and exit."
    exit 1
}

###############################################################################
# Parse command-line arguments
###############################################################################
nodes=()
rf=""
clean=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes|--node|-n)
            shift
            if [[ -z "${1:-}" ]]; then
                echo "Error: Missing argument for --nodes"
                usage
            fi
            IFS=',' read -ra PORTS <<< "$1"
            for p in "${PORTS[@]}"; do
                nodes+=("$p")
            done
            ;;
        --rf)
            shift
            if [[ -z "${1:-}" ]]; then
                echo "Error: Missing argument for --rf"
                usage
            fi
            rf="$1"
            ;;
        --clean)
            clean=true
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
    shift
done

if [ "${#nodes[@]}" -eq 0 ]; then
    echo "Error: You must supply at least one --nodes argument."
    usage
fi

# Set default replication factor if not provided: (#nodes / 2) + 1
if [ -z "$rf" ]; then
    count=${#nodes[@]}
    rf=$(( (count / 2) + 1 ))
fi

echo "Launching cluster with nodes on ports: ${nodes[*]}"
echo "Replication Factor: $rf"

###############################################################################
# Use absolute path for configctl command (assumed relative to this script)
###############################################################################
CONFIGCTL="configctl/configctl"

###############################################################################
# Determine installer status (using a .bin installer)
###############################################################################
installer_dir="../concourse-server/build/distributions"
existing_installer=$(find "$installer_dir" -maxdepth 1 -type f -name "concourse-server*.bin" | head -n 1)

if [ -z "${clean:-}" ] && [ -n "$existing_installer" ]; then
    installer_sh="$existing_installer"
    echo "Using existing installer: $installer_sh"
else
    echo "Building installer..."
    pushd ".." > /dev/null
    ./gradlew clean installer
    popd > /dev/null

    installer_sh=$(find "$installer_dir" -maxdepth 1 -type f -name "concourse-server*.bin" | head -n 1)
    if [ -z "$installer_sh" ]; then
        echo "Error: Installer not found in $installer_dir after building."
        exit 1
    fi
    echo "Installer built: $installer_sh"
fi

###############################################################################
# Helper functions and variables (Bash 3.x compatible using indexed arrays)
###############################################################################
node_dirs=()   # Array to hold temp directories per node index
tail_pids=()   # Array to hold tail process PIDs per node index

# Function to find an available port (requires lsof and shuf)
get_free_port() {
    local port
    while :; do
        if command -v shuf >/dev/null 2>&1; then
            port=$(shuf -i 2000-65000 -n 1)
        else
            port=$(jot -r 1 2000 65000)
        fi
        if ! lsof -i :"$port" &>/dev/null; then
            echo "$port"
            return
        fi
    done
}

# Build the full cluster nodes list (each as localhost:port)
cluster_nodes=()
for port in "${nodes[@]}"; do
    cluster_nodes+=("localhost:$port")
done

###############################################################################
# Cleanup function to stop nodes, kill log tails, and remove temporary directories
###############################################################################
cleanup() {
    echo -e "\nStopping all nodes..."
    for (( i=0; i<${#nodes[@]}; i++ )); do
        node_port="${nodes[$i]}"
        tmp_dir="${node_dirs[$i]}"
        echo "Stopping node running on port $node_port..."
        if [ -x "$tmp_dir/concourse-server/bin/concourse" ]; then
            (cd "$tmp_dir/concourse-server/bin" && ./concourse stop)
        fi
    done

    echo "Terminating log tail processes..."
    for (( i=0; i<${#tail_pids[@]}; i++ )); do
        kill "${tail_pids[$i]}" 2>/dev/null || true
    done

    echo "Cleaning up temporary directories..."
    for (( i=0; i<${#node_dirs[@]}; i++ )); do
        rm -rf "${node_dirs[$i]}"
    done

    exit 0
}
trap cleanup SIGINT

###############################################################################
# Loop to create, configure, and start each node
###############################################################################
# Add arrays to store debug ports and directories for the summary
debug_ports=()
jmx_ports=()

for (( i=0; i<${#nodes[@]}; i++ )); do
    node_port="${nodes[$i]}"
    echo "Setting up node on port $node_port..."
    tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/concourse_node.XXXXXX")
    node_dirs[$i]="$tmp_dir"
    echo "Node directory: $tmp_dir"

    # Copy installer and run it to create the node installation (creates concourse-server/)
    cp "$installer_sh" "$tmp_dir/"
    (cd "$tmp_dir" && sh concourse-server*bin -- skip-integration)

    # The configuration file is now under concourse-server/conf/concourse.yaml
    config_file="$tmp_dir/concourse-server/conf/concourse.yaml"
    if [ ! -f "$config_file" ]; then
        echo "Error: Config file not found at $config_file"
        exit 1
    fi

    # Set the cluster nodes list in the config (each node gets the complete list)
    for (( j=0; j<${#cluster_nodes[@]}; j++ )); do
        address="${cluster_nodes[$j]}"
        "$CONFIGCTL" write -k "cluster.nodes.$j" -v "$address" -f "$config_file"
    done

    "$CONFIGCTL" write -k "cluster.replication_factor" -v "$rf" -f "$config_file"

    # Select a free port for remote debugging and set it
    debugger_port=$(get_free_port)
    jmx_port=$(get_free_port)
    "$CONFIGCTL" write -k "remote_debugger_port" -v "$debugger_port" -f "$config_file"
    "$CONFIGCTL" write -k "jmx_port" -v "$jmx_port" -f "$config_file" 
    echo "Node on port $node_port will listen for remote debugging on port $debugger_port"

    "$CONFIGCTL" write -k "log_level" -v "DEBUG" -f "$config_file"
    "$CONFIGCTL" write -k "client_port" -v "$node_port" -f "$config_file"

    # Prepare full paths for buffer and DB directories (create if necessary)
    mkdir -p "$tmp_dir/data/buffer" "$tmp_dir/data/db"
    "$CONFIGCTL" write -k "buffer_directory" -v "$(realpath "$tmp_dir/data/buffer")" -f "$config_file"
    "$CONFIGCTL" write -k "database_directory" -v "$(realpath "$tmp_dir/data/db")" -f "$config_file"

    # Start the node (it runs as a daemon)
    (cd "$tmp_dir/concourse-server/bin" && ./concourse start)

    # Tail the node's log file (assuming logs are in concourse-server/logs)
    log_file=$(find "$tmp_dir/concourse-server/log" -type f | head -n 1 || true)
    if [ -n "$log_file" ]; then
        tail -f "$log_file" | sed "s/^/[Node $node_port] /" &
        tail_pids[$i]=$!
    else
        echo "Warning: No log file found for node on port $node_port"
    fi

    # Store debug and jmx ports for summary
    debug_ports[$i]=$debugger_port
    jmx_ports[$i]=$jmx_port
done

###############################################################################
# Print summary table
###############################################################################
echo -e "\nCluster Node Summary:"
printf "%-15s %-15s %-15s %s\n" "PORT" "DEBUG PORT" "JMX PORT" "DIRECTORY"
printf "%-15s %-15s %-15s %s\n" "----" "----------" "--------" "---------"
for (( i=0; i<${#nodes[@]}; i++ )); do
    printf "%-15s %-15s %-15s %s\n" "${nodes[$i]}" "${debug_ports[$i]}" "${jmx_ports[$i]}" "${node_dirs[$i]}"
done
echo -e "\nAll nodes started. Press Ctrl+C to stop the cluster."

# Wait indefinitely so that the trap can capture Ctrl+C
while true; do sleep 1; done

exit 0
