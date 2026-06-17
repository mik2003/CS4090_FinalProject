import sys
import asyncio
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from netqasm.runtime.settings import set_simulator

set_simulator("simulaqron")

from netqasm.sdk import EPRSocket, Qubit

from simulaqron.settings import network_config, simulaqron_settings
from simulaqron.settings.network_config import NodeConfigType

from helper import *
from node import NetworkNode

# States
STATE_INIT = "INIT"
STATE_READY = "READY"
STATE_INIT_GHZ = "INIT_GHZ"
STATE_EPR_MERGED = "EPR_MERGED"

# Commands
CMD_NEW_NODE = "NEW_NODE"
CMD_START = "START"
CMD_RECV_EPR = "RECV_EPR"
CMD_MERGE_EPR = "MERGE_EPR"

# Shared mutable states across the event loop
state = STATE_INIT
q: Qubit | None = None
ancilla: Qubit | None = None

# Node info
node = NetworkNode(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
role = (
    sys.argv[2].strip().upper() if len(sys.argv) > 2 else "N"
)  # Sender(S), Receiver(R), or Node(N)
is_first_node = node.index == 0
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
    global q, ancilla

    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    # Executes in the last node

    # When we receive a START command in the READY state, we can initiate the GHZ state creation
    log("Every node is now ready. Starting EPR pairs creation...")

    # Create an EPR pair using our qubit
    q = prev_node_socket().create_keep()[0]
    node.quantum_connection.flush()

    send_to_prev_node(CMD_RECV_EPR)

    return STATE_INIT_GHZ


async def handle_recv_epr(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q, ancilla

    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    if is_first_node:
        # Receive the EPR from the next node, now in the standard qubit
        q = next_node_socket().recv_keep()[0]
        node.quantum_connection.flush()

        # Every node now has at least one EPR pair
        # We now need to merge them
        log("Created all EPRs pairs. Starting merge...")

        # Do not apply any correction, we are initiating the chain
        send_to_next_node(CMD_MERGE_EPR, "0")
        return STATE_EPR_MERGED
    else:
        # Unless it's the first node, repeat the process
        # Receive the EPR from the next node
        ancilla = next_node_socket().recv_keep()[0]
        log("Received EPR pair from next node")
        q = prev_node_socket().create_keep()[0]
        log("Sent EPR pair to previous node")
        node.quantum_connection.flush()

        send_to_prev_node(CMD_RECV_EPR)
        return STATE_INIT_GHZ


async def handle_epr_merge(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q, ancilla

    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    if q is None:
        raise ValueError("Qubit not initialized")

    # Retrieve the correction from previous node merge
    correction = int(argument)
    if correction == 1:
        q.X()

    if is_last_node:
        log("GHZ state completed.")
        # TODO: do something with the GHZ state
        return STATE_EPR_MERGED

    if ancilla is None:
        raise ValueError("Ancilla qubit not initialized")

    # Apply the EPR (and GHZ) merge circuit, merging the previous
    # GHZ state with the next EPR one
    q.cnot(ancilla)

    m = ancilla.measure()
    node.quantum_connection.flush()
    m = int(m)

    # Send to the next node
    send_to_next_node(CMD_MERGE_EPR, f"{m}")
    return STATE_EPR_MERGED


# ---- Communication ------------------------------------------------


def prev_node():
    if is_first_node:
        return node.connections[len(node.connections) - 1]
    else:
        return node.connections[node.index - 1]


def next_node():
    if is_last_node:
        return node.connections[0]
    else:
        return node.connections[node.index + 1]


def send_to_prev_node(command: str, argument: str = "") -> None:
    log(f"Sending '{command} {argument}' to previous node...")
    prev_node().send_classical(command, argument)


def send_to_next_node(command: str, argument: str = "") -> None:
    log(f"Sending '{command} {argument}' to next node...")
    next_node().send_classical(command, argument)


def prev_node_socket() -> EPRSocket:
    return prev_node().epr_socket


def next_node_socket() -> EPRSocket:
    return next_node().epr_socket


# ---- Handlers map -------------------------------------------------


EVENT_HANDLERS = {
    (STATE_INIT, CMD_NEW_NODE): handle_new_node,
    (STATE_INIT, CMD_START): handle_start,
    (STATE_READY, CMD_START): handle_ready_state,
    (STATE_READY, CMD_RECV_EPR): handle_recv_epr,
    (STATE_INIT_GHZ, CMD_MERGE_EPR): handle_epr_merge,
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
        node.complete_network()

        # Wait for a moment to ensure the node is connected
        asyncio.run(asyncio.sleep(1))

        send_to_next_node(CMD_NEW_NODE)
        send_to_next_node(CMD_START)

        state = STATE_READY
        log(f"Completed network setup. This is the last node.")

    # Wait for all threads to finish (this will block the main thread)
    # Keep main thread alive until all threads complete
    if node.thread:
        node.thread.join()
    for conn in node.connections.values():
        if conn.thread:
            conn.thread.join()
