import sys
import asyncio
import numpy as np
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
STATE_ANON_TRANSMIT = "ANON_TRANSMIT"
STATE_ANON_ENTANGLEMENT = "ANON_ENTANGLEMENT"
STATE_TRANSMIT_DONE = "TRANSMIT_DONE"
STATE_ENTANGLEMENT_DONE = "ENTANGLEMENT_DONE"

# Commands
CMD_NEW_NODE = "NEW_NODE"
CMD_START = "START"
CMD_RECV_EPR = "RECV_EPR"
CMD_MERGE_EPR = "MERGE_EPR"
CMD_ANON_TRANSMIT = "ANON_TRANSMIT"
CMD_ANON_ENTANGLEMENT = "ANON_ENTANGLEMENT"
CMD_TRANSMIT_RESULT = "TRANSMIT_RESULT"
CMD_ENTANGLEMENT_COMPLETED = "ENTANGLEMENT_COMPLETED"

# To perform anonymous QKD, we start by teleporting random BB84 states
# from the send to the receiver. Each teleportation uses:
# - One round of anonymous entanglement
# - Two rounds of classical communication for the corrections
# This procedure is repeated several times. Each time, the receiver measures
# the teleported state in a random basis.
# When enough states have been teleported, the sender shares the bases
# used for each state, and the receiver can discard the states teleported
# in the wrong basis. This takes several rounds of anonymous communication.

# Procedures and phases are higher order states that determine what the network
# should do next.

# Procedures
PROC_ANON_TRANSMIT = "ANON_TRANSMIT"
PROC_ANON_ENTANGLEMENT = "ANON_ENTANGLEMENT"

# Phases
PHASE_BB84_DISTRIBUTION = "BB84_DISTRIBUTION"
PHASE_BASIS_RECONCILIATION = "BASIS_RECONCILIATION"


# Shared mutable states across the event loop
state = STATE_INIT
procedure = PROC_ANON_TRANSMIT
phase = PHASE_BB84_DISTRIBUTION

q: Qubit | None = None
ancilla: Qubit | None = None
memory: Qubit | None = None  # Temporarily store the qubit waiting for corrections
rng = np.random.default_rng()

bit_to_send: int | None = 1

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
        log(f"Connected to unexpected node index {conn_node_index}")

    node.connect_to_node(conn_node_index, event_loop)

    return STATE_INIT


async def handle_start_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    # Broadcast START across the network to ensure all nodes are ready

    if is_last_node:
        # When we receive a START command in the READY state, we can initiate the GHZ state creation
        log("Every node is now ready. Starting EPR pairs creation...")
        send_to_next_node(CMD_RECV_EPR)
    else:
        log("Received START command. Completing network.")
        send_to_next_node(CMD_START)
        node.complete_network()
        log("Completed network setup.")

    return STATE_READY


async def handle_recv_epr_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q, ancilla

    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    if is_first_node:
        # Start the chain
        log("Starting EPRs creation...")
        q = next_node_socket().create_keep()[0]
        node.quantum_connection.flush()

        # Do not apply any correction, we are initiating the chain
        send_to_next_node(CMD_RECV_EPR)
    elif is_last_node:
        # Receive the EPR from the previous node, now in the standard qubit
        q = prev_node_socket().recv_keep()[0]
        node.quantum_connection.flush()

        # Every node now has at least one EPR pair
        # We now need to merge them
        log("Created all EPRs pairs. Starting merge...")

        # Start the merge process
        send_to_next_node(CMD_MERGE_EPR)
    else:
        # Unless it's the first or last node, repeat the process
        # Receive the EPR from the prev node
        ancilla = prev_node_socket().recv_keep()[0]
        log("Received EPR pair from previous node")
        q = next_node_socket().create_keep()[0]
        log("Sent EPR pair to next node")
        node.quantum_connection.flush()

        send_to_next_node(CMD_RECV_EPR)

    return STATE_INIT_GHZ


async def handle_epr_merge_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q, ancilla

    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    if q is None:
        raise ValueError("Qubit not initialized")

    if is_first_node:
        # Do nothing if it's the first node
        send_to_next_node(CMD_MERGE_EPR)
        return STATE_EPR_MERGED

    # Retrieve the correction from previous node merge
    correction = int(argument) if argument != "" else 0
    if correction == 1:
        q.X()

    if is_last_node:
        log("GHZ state completed.")
        # Start current procedure
        if procedure == PROC_ANON_ENTANGLEMENT:
            send_to_next_node(CMD_ANON_ENTANGLEMENT)
        else:
            send_to_next_node(CMD_ANON_TRANSMIT)
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


async def handle_anon_transmit_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q

    if q is None:
        raise ValueError("Qubit not initialized")
    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    parity = int(argument) if argument != "" else 0

    log(f"Performing anonymous transmission. Parity is {parity}")

    # Sender applies Z to their qubit
    if role == "S":
        if bit_to_send:
            q.Z()
    # Every player applies H
    q.H()
    # Measure and update parity
    m = q.measure()
    node.quantum_connection.flush()
    m = int(m)

    parity = parity ^ m

    if is_last_node:
        # Broadcast the result across the network
        send_to_next_node(CMD_TRANSMIT_RESULT, f"{parity}")
    else:
        send_to_next_node(CMD_ANON_TRANSMIT, f"{parity}")

    return STATE_TRANSMIT_DONE


async def handle_anon_entanglement_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q

    if q is None:
        raise ValueError("Qubit not initialized")
    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    parity = int(argument) if argument != "" else 0

    log(f"Performing anonymous entanglement. Parity is {parity}")

    # Every standard node applies H, measures and broadcasts parity
    if role == "N":
        q.H()
        m = q.measure()
        node.quantum_connection.flush()
        m = int(m)
        parity = parity ^ m
    # The sender picks a random bit, and applies Z^b, measures and broadcasts
    elif role == "S":
        b = rng.integers(0, 2)
        if b:
            q.Z()
        parity = parity ^ b
    # The receiver applies X^parity
    elif role == "R":
        if parity:
            q.X()

    if is_last_node:
        send_to_next_node(CMD_ENTANGLEMENT_COMPLETED)
    else:
        send_to_next_node(CMD_ANON_ENTANGLEMENT, f"{parity}")

    return STATE_ENTANGLEMENT_DONE


async def handle_transmit_result(
    writer: StreamWriter, conn_node_index: int, argument: str
):
    result = int(argument)
    log(f"Transmission result is {result}")
    send_to_next_node(CMD_TRANSMIT_RESULT, f"{result}")

    # TODO:
    # If receiver do something with the transmitted data
    # This includes:
    # - Apply corrections to the state for teleportation
    # - Save BB84 bases
    # - Privacy Amplification

    if is_last_node:
        # TODO: Update procedure and phase
        # Stop the program if done
        pass

    return STATE_READY


async def handle_entanglement_completed(
    writer: StreamWriter, conn_node_index: int, argument: str
):
    log("Entanglement completed")
    send_to_next_node(CMD_ENTANGLEMENT_COMPLETED)

    # TODO:
    # If sender perform teleportation circuit and prepare
    # corrections to send in the next phase

    # If receiver save the qubit into `memory` waiting for corrections

    if is_last_node:
        # TODO: Update procedure and phase
        # Stop the program if done
        pass

    return STATE_READY


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
    (STATE_INIT, CMD_NEW_NODE): handle_new_node,  # Connect to new node
    (STATE_INIT, CMD_START): handle_start_broadcast,  # Complete networks for all nodes
    (STATE_READY, CMD_RECV_EPR): handle_recv_epr_broadcast,  # Distribute EPRs
    (STATE_INIT_GHZ, CMD_MERGE_EPR): handle_epr_merge_broadcast,  # Merge EPRs
    (STATE_EPR_MERGED, CMD_ANON_TRANSMIT): handle_anon_transmit_broadcast,
    (STATE_EPR_MERGED, CMD_ANON_ENTANGLEMENT): handle_anon_entanglement_broadcast,
    (STATE_TRANSMIT_DONE, CMD_TRANSMIT_RESULT): handle_transmit_result,
    (
        STATE_ENTANGLEMENT_DONE,
        CMD_ENTANGLEMENT_COMPLETED,
    ): handle_entanglement_completed,
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

        log(f"Transitioning to state {state}")


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

        log(f"Completed network setup. This is the last node.")

    # Wait for all threads to finish (this will block the main thread)
    # Keep main thread alive until all threads complete
    if node.thread:
        node.thread.join()
    for conn in node.connections.values():
        if conn.thread:
            conn.thread.join()
