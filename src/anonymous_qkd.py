import sys
import asyncio
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from netqasm.runtime.settings import set_simulator

set_simulator("simulaqron")

from netqasm.sdk import EPRSocket

from simulaqron.settings import network_config, simulaqron_settings
from simulaqron.settings.network_config import NodeConfigType

from helper import *
from node import NetworkNode

# States
STATE_INIT = "INIT"
STATE_READY = "READY"
STATE_INIT_GHZ = "INIT_GHZ"

# Commands
CMD_NEW_NODE = "NEW_NODE"
CMD_START = "START"
CMD_INIT_GHZ = "INIT_GHZ"

# Shared mutable states across processes
state = STATE_INIT

# Node info
node = NetworkNode(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
role = (
    sys.argv[2].strip().upper() if len(sys.argv) > 2 else "N"
)  # Sender(S), Receiver(R), or Node(N)
is_last_node = sys.argv[3].strip().upper() == "LAST" if len(sys.argv) > 3 else False

# Check for valid role
if role not in ["S", "R", "N"]:
    raise ValueError("Invalid role. Must be 'S', 'R', or 'N'.")


def log(message: str) -> None:
    print(
        f"Node({node.index}) [{role}] [{state}]: {message}",
        flush=True,
    )


# ---- Connections Manager ------------------------------------------


def determine_node_type(conn_node_index: int) -> str:
    if conn_node_index == node.index - 1:
        return "PREV"
    elif conn_node_index == node.index + 1:
        return "NEXT"
    else:
        return "OTHER"


# ---- Event handlers -----------------------------------------------


async def handle_new_node(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:

    if conn_node_index == node.index + 1:
        log(f"Connected to next node (Node{conn_node_index})")
    elif node.index == 0:  # Connecting first to last node
        log(f"Connected to previous node (Node{conn_node_index})")
    else:
        log(f"Connected to unexpected node index {conn_node_index} - ignoring")

    node.connect_to_node(conn_node_index, event_loop)

    return STATE_INIT


async def handle_start(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    # Broadcast START across the network to ensure all nodes are ready
    log("Received START command. Transitioning to READY state.")
    send_to_next_node(CMD_START)
    node.complete_network()
    log("Completed network setup.")

    return STATE_READY


async def handle_ready_state(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    # When we receive a START command in the READY state, we can initiate the GHZ state creation
    log("Every node is now ready. Starting GHZ state initialization.")
    send_to_next_node(CMD_INIT_GHZ)

    return STATE_INIT_GHZ


# ---- Communication ------------------------------------------------


def send_to_prev_node(command: str, argument: str = "") -> None:
    log(f"Sending '{command} {argument}' to previous node...")
    if node.index == 0:
        # If this is the first node, send to the last node to complete the ring
        node.connections[len(node.connections) - 1].send_classical(command, argument)
    else:
        node.connections[node.index - 1].send_classical(command, argument)


def send_to_next_node(command: str, argument: str = "") -> None:
    log(f"Sending '{command} {argument}' to next node...")
    if is_last_node:
        # If this is the last node, send to Node0 to complete the ring
        node.connections[0].send_classical(command, argument)
    else:
        node.connections[node.index + 1].send_classical(command, argument)


def prev_node_socket() -> EPRSocket:
    return node.connections[node.index - 1].epr_socket


def next_node_socket() -> EPRSocket:
    return node.connections[node.index + 1].epr_socket


# ---- Handlers map -------------------------------------------------


EVENT_HANDLERS = {
    (STATE_INIT, CMD_NEW_NODE): handle_new_node,
    (STATE_INIT, CMD_START): handle_start,
    (STATE_READY, CMD_START): handle_ready_state,
}


# ---- Event loop ---------------------------------------------------


async def event_loop(reader: StreamReader, writer: StreamWriter) -> None:
    global state

    while True:
        data = await reader.readline()
        msg = data.decode()

        log(f"Received message: '{msg.strip()}'")

        try:
            command, conn_node_index, argument = parse_message(msg)
        except ValueError as e:
            log(f"Error parsing message '{msg.strip()}': {e}")
            return

        matching = (state, command)
        handler = EVENT_HANDLERS.get(matching)

        if handler is None:
            log(f"no transition for '{matching}' - ignoring")
            return

        state = await handler(
            writer,
            conn_node_index,
            argument,
        )


# ---- Main ---------------------------------------------------------

if __name__ == "__main__":
    _here = Path(__file__).parent
    simulaqron_settings.read_from_file(_here / "simulaqron_settings.json")
    network_config.read_from_file(_here / "simulaqron_network.json")

    node.start_classical_server(event_loop)

    if node.index > 0:
        log(f"Connecting to previous node (Node{node.index - 1})...")
        node.connect_to_node(node.index - 1, event_loop)

        # Wait for a moment to ensure the node is connected
        asyncio.run(asyncio.sleep(1))

        send_to_prev_node(CMD_NEW_NODE)
    if is_last_node:
        log(f"Connecting to next node (Node0) to complete the ring...")
        node.connect_to_node(0, event_loop)

        # Wait for a moment to ensure the node is connected
        asyncio.run(asyncio.sleep(1))

        send_to_next_node(CMD_NEW_NODE)
        send_to_next_node(CMD_START)

        state = STATE_READY
        node.complete_network()
        log(f"Completed network setup. This is the last node.")

    # Wait for all threads to finish (this will block the main thread)
    # Keep main thread alive until all threads complete
    if node.thread:
        node.thread.join()
    for conn in node.connections.values():
        if conn.thread:
            conn.thread.join()
