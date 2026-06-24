import asyncio
import os
import sys
import threading
from asyncio import StreamReader, StreamWriter
from pathlib import Path

import numpy as np
from helper import parse_message
from netqasm.runtime.settings import set_simulator

set_simulator("simulaqron")

from netqasm.sdk import EPRSocket, Qubit  # noqa: E402
from node import NetworkNode  # noqa: E402
from simulaqron.settings import network_config, simulaqron_settings  # noqa: E402

# States
STATE_INIT = "INIT"
STATE_READY = "READY"
STATE_INIT_GHZ = "INIT_GHZ"
STATE_EPR_MERGED = "EPR_MERGED"
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
CMD_STOP = "STOP"

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
PHASE_RECON_SENDER_BASES = "RECON_SENDER_BASES"  # sender announces theta_i
PHASE_RECON_RECEIVER_BASES = "RECON_RECEIVER_BASES"  # receiver announces phi_i
PHASE_DONE = "DONE"

# Substeps within one BB84 symbol during distribution
SUB_ENT = "ENT"  # anonymous entanglement + teleport
SUB_TX_X = "TX_X"  # broadcast X-correction bit b
SUB_TX_Z = "TX_Z"  # broadcast Z-correction bit a, then R measures


# ---- Pipeline parameters --------------------------------------
NUM_SYMBOLS = 32  # BB84 states to teleport; sifted key ~ NUM_SYMBOLS/2

# ---- Shared mutable states across the event loop -------------------

state = STATE_INIT
procedure = PROC_ANON_ENTANGLEMENT
phase = PHASE_BB84_DISTRIBUTION

q: Qubit | None = None
ancilla: Qubit | None = None
memory: Qubit | None = None  # Temporarily store the qubit waiting for corrections
rng = np.random.default_rng()


# ---- Protocol schedule ---------------------------------------------

symbol_index = 0
substep = SUB_ENT
recon_index = 0

# -- Sender ("S") private data --
sender_bits: list[int] = []
sender_bases: list[int] = []
pending_corr: tuple[int, int] | None = None  # (b = X-corr, a = Z-corr)
recv_bases_seen: list[int] = []  # phi_i learned during receiver-basis recon

# -- Receiver ("R") private data --
recv_bases: list[int] = []
recv_outcomes: list[int] = []
sender_bases_seen: list[int] = []  # theta_i learned during sender-basis recon

# -- Shared / outputs --
raw_key = None  # x_A on sender, x_B on receiver
final_key = None
_shutting_down = False


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
    # Build a context label, including only the parts that are meaningful now.
    ctx = [f"Node({node.index})", f"[{role}]", f"[{state}]"]

    # Schedule info is only meaningful once we're past network setup.
    if (
        state not in (STATE_INIT, STATE_READY)
        or phase != PHASE_BB84_DISTRIBUTION
        or symbol_index > 0
    ):
        if phase == PHASE_BB84_DISTRIBUTION:
            ctx.append(f"[{phase} sym={symbol_index}/{NUM_SYMBOLS} {substep}]")
        elif phase in (PHASE_RECON_SENDER_BASES, PHASE_RECON_RECEIVER_BASES):
            ctx.append(f"[{phase} {recon_index}/{NUM_SYMBOLS}]")
        else:  # PHASE_DONE
            ctx.append(f"[{phase}]")

    print(" ".join(ctx) + f": {message}", flush=True)


# ---- Schedule helpers ---------------------------------------------


def current_round_is_entanglement() -> bool:
    return phase == PHASE_BB84_DISTRIBUTION and substep == SUB_ENT


def current_transmitter() -> str:
    """Which role ENCODES during the current anonymous-transmission round."""
    if phase == PHASE_RECON_RECEIVER_BASES:
        return "R"
    return "S"  # corrections, sender bases


def next_outgoing_bit() -> int:
    """The single bit the transmitter encodes in the current ANON_TRANSMIT lap."""
    if phase == PHASE_BB84_DISTRIBUTION:
        assert pending_corr is not None
        return pending_corr[0] if substep == SUB_TX_X else pending_corr[1]
    if phase == PHASE_RECON_SENDER_BASES:
        return sender_bases[recon_index]
    if phase == PHASE_RECON_RECEIVER_BASES:
        return recv_bases[recon_index]
    return 0


# ---- Event handlers -----------------------------------------------


async def handle_new_node(writer: StreamWriter, conn_node_index: int, argument: str) -> str:

    if conn_node_index == node.index + 1:
        log(f"Connected to next node (Node{conn_node_index})")
    elif node.index == 0:  # Connecting first to last node
        log(f"Connected to previous node (Node{conn_node_index})")
    else:
        log(f"Connected to unexpected node index {conn_node_index}")

    node.connect_to_node(conn_node_index, event_loop)

    return STATE_INIT


async def handle_start_broadcast(writer: StreamWriter, conn_node_index: int, argument: str) -> str:
    # Broadcast START across the network to ensure all nodes are ready

    if is_last_node:
        # When we receive a START command in the READY state,
        # we can initiate the GHZ state creation
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

    # Apply correction from the previous merge.
    # For intermediate nodes, the correction belongs to the qubit received
    # from the previous node, i.e. ancilla. For the last node, that qubit is q.
    correction = int(argument) if argument != "" else 0
    if correction == 1:
        if is_last_node:
            q.X()
        else:
            ancilla.X()

    if is_last_node:
        log("GHZ state completed.")
        # Start current procedure
        if procedure == PROC_ANON_ENTANGLEMENT:
            send_to_next_node(CMD_ANON_ENTANGLEMENT)
        else:
            send_to_next_node(CMD_ANON_TRANSMIT)
        return STATE_EPR_MERGED

    # Merge previous GHZ half with the new EPR half.
    # Existing GHZ/previous qubit = ancilla
    # New local EPR half = q
    ancilla.cnot(q)

    m = q.measure()
    node.quantum_connection.flush()
    m = int(m)

    # Keep the previous/GHZ qubit as this node's protocol qubit.
    q = ancilla
    ancilla = None

    send_to_next_node(CMD_MERGE_EPR, f"{m}")

    return STATE_EPR_MERGED


async def handle_anon_transmit_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    """GHZ-based anonymous broadcast of ONE bit from current_transmitter()"""

    global q

    if q is None:
        raise ValueError("Qubit not initialized")
    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    parity = int(argument) if argument != "" else 0

    log(f"Performing anonymous transmission. Parity is {parity}")

    # Only the designated transmitter encodes (sender for most phases,
    # receiver while it announces its bases).
    if role == current_transmitter():
        log(f"anon tx intended bit = {next_outgoing_bit()}")
    if role == current_transmitter() and next_outgoing_bit():
        q.Z()

    # Every node applies H
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
    """
    Establish anonymous entanglement: plain nodes measure in X and disentangle;
    the sender masks with a random Z^b; the receiver KEEPS its qubit and applies
    the Z correction later (in handle_entanglement_completed) on the full parity.
    """

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
    # role == "R": keep q untouched; correction happens on the completion lap.

    if is_last_node:
        send_to_next_node(CMD_ENTANGLEMENT_COMPLETED, f"{parity}")
    else:
        send_to_next_node(CMD_ANON_ENTANGLEMENT, f"{parity}")

    return STATE_ENTANGLEMENT_DONE


async def handle_transmit_result(writer: StreamWriter, conn_node_index: int, argument: str):
    global memory

    result = int(argument)

    # role action for the CURRENT round
    if phase == PHASE_BB84_DISTRIBUTION and role == "R":
        if substep == SUB_TX_X:
            if result and memory is not None:
                memory.X()  # X^b
                node.quantum_connection.flush()
        elif substep == SUB_TX_Z and memory is not None:
            if result:
                memory.Z()  # Z^a
            phi = rng.integers(0, 2)  # receiver's random measurement basis
            if phi:
                memory.H()
            outcome = memory.measure()
            node.quantum_connection.flush()
            recv_bases.append(phi)
            recv_outcomes.append(int(outcome))
            memory = None
            log(f"Measured teleported state: basis={phi}, outcome={recv_outcomes[-1]}")

    elif phase == PHASE_RECON_SENDER_BASES and role == "R":
        sender_bases_seen.append(result)

    elif phase == PHASE_RECON_RECEIVER_BASES and role == "S":
        recv_bases_seen.append(result)

    if is_last_node:
        advance_schedule()
        conductor_next()
    else:
        send_to_next_node(CMD_TRANSMIT_RESULT, f"{result}")
        advance_schedule()

    return STATE_READY


async def handle_entanglement_completed(writer: StreamWriter, conn_node_index: int, argument: str):
    global q, memory, pending_corr

    final_parity = int(argument) if argument != "" else 0

    if role == "R":
        # If receiver save the qubit into `memory` waiting for corrections
        if final_parity:
            q.Z()
        node.quantum_connection.flush()
        memory = q
        q = None
        log("Stored Bell half; awaiting teleport corrections")

    elif role == "S":
        # Prepare random BB84 state |psi> and teleport it through q
        v = int(rng.integers(0, 2))  # bit value
        theta = int(rng.integers(0, 2))  # 0 = Z basis, 1 = X basis
        psi = Qubit(node.quantum_connection)
        if v:
            psi.X()
        if theta:
            psi.H()
        # Teleport |psi> through anonymous Bell pair
        psi.cnot(q)
        psi.H()
        a_f = psi.measure()
        b_f = q.measure()
        node.quantum_connection.flush()
        a = int(a_f)
        b = int(b_f)
        q = None
        sender_bits.append(v)
        sender_bases.append(theta)
        pending_corr = (b, a)
        log(f"Teleported BB84 (v={v}, basis={theta}); corr X:{b} Z:{a}")

    if is_last_node:
        advance_schedule()
        conductor_next()
    else:
        send_to_next_node(CMD_ENTANGLEMENT_COMPLETED, f"{final_parity}")
        advance_schedule()

    return STATE_READY


# ---- Schedule advance (runs on EVERY node, once per round) --------


def advance_schedule() -> None:
    global phase, symbol_index, substep, recon_index

    if phase == PHASE_BB84_DISTRIBUTION:
        if substep == SUB_ENT:
            substep = SUB_TX_X
        elif substep == SUB_TX_X:
            substep = SUB_TX_Z  
        else:  # finished one symbol
            symbol_index += 1
            if symbol_index < NUM_SYMBOLS:
                substep = SUB_ENT
            else:
                phase = PHASE_RECON_SENDER_BASES
                recon_index = 0

    elif phase == PHASE_RECON_SENDER_BASES:
        recon_index += 1
        if recon_index >= NUM_SYMBOLS:
            phase = PHASE_RECON_RECEIVER_BASES
            recon_index = 0

    elif phase == PHASE_RECON_RECEIVER_BASES:
        recon_index += 1
        if recon_index >= NUM_SYMBOLS:
            _sift_only()
            phase = PHASE_DONE


# ---- Conductor (last node only): start next round or stop ---------


def conductor_next() -> None:
    global procedure
    if phase == PHASE_DONE:
        log("Protocol complete -- broadcasting STOP")
        send_to_next_node(CMD_STOP)
        _schedule_shutdown()
        return
    procedure = PROC_ANON_ENTANGLEMENT if current_round_is_entanglement() else PROC_ANON_TRANSMIT
    send_to_next_node(CMD_RECV_EPR)  # rebuild a fresh GHZ for the next round


# ---- Classical back-half (sift) --------------------------


def _kept_indices(bases_a: list[int], bases_b: list[int]) -> list[int]:
    return [i for i in range(NUM_SYMBOLS) if bases_a[i] == bases_b[i]]


def _sift_only() -> None:
    """Sift to matching bases. Noiseless sim => x_A == x_B, so the sifted
    string is the shared key directly."""
    global raw_key, final_key

    if role == "S":
        log(f"sender_bases      = {sender_bases}")
        log(f"recv_bases_seen   = {recv_bases_seen}")
        kept = _kept_indices(sender_bases, recv_bases_seen)
        key = np.array([sender_bits[i] for i in kept], dtype=np.uint8)
    elif role == "R":
        log(f"sender_bases_seen = {sender_bases_seen}")
        log(f"recv_bases        = {recv_bases}")
        kept = _kept_indices(sender_bases_seen, recv_bases)
        key = np.array([recv_outcomes[i] for i in kept], dtype=np.uint8)
    else:
        return

    log(f"kept = {kept}")

    raw_key = final_key = key
    log(f"sifted {len(kept)}/{NUM_SYMBOLS} -> key of {len(key)} bits")
    log("final key " + np.array2string(key, max_line_width=10000))


# ---- STOP / shutdown ----------------------------------------------


def _schedule_shutdown() -> None:
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    # Blunt but effective for a demo: let the in-flight STOP lap finish, then
    # exit so the main thread's join() calls unblock. A cleaner version would
    # set an asyncio.Event and break the event loop.
    threading.Timer(1.5, lambda: os._exit(0)).start()


async def handle_stop(writer: StreamWriter, conn_node_index: int, argument: str) -> str:
    log("Received STOP -- shutting down")
    if role in ("S", "R") and final_key is not None:
        log(
            f"FINAL KEY ({len(final_key)} bits): "
            + np.array2string(np.asarray(final_key), max_line_width=10000)
        )
    if not is_last_node:
        send_to_next_node(CMD_STOP)
    _schedule_shutdown()
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
    (STATE_ENTANGLEMENT_DONE, CMD_ENTANGLEMENT_COMPLETED): handle_entanglement_completed,
    (STATE_READY, CMD_STOP): handle_stop,
}


# ---- Event loop ---------------------------------------------------


async def event_loop(reader: StreamReader, writer: StreamWriter) -> None:
    global state

    while True:
        data = await reader.readline()

        if not data:  # EOF: peer closed the connection
            log("Connection closed by peer")
            return

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
            continue

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
        log("Connecting to next node (Node0) to complete the ring...")
        node.connect_to_node(0, event_loop)
        node.complete_network()

        # Wait for a moment to ensure the node is connected
        asyncio.run(asyncio.sleep(1))

        send_to_next_node(CMD_NEW_NODE)
        send_to_next_node(CMD_START)

        log("Completed network setup. This is the last node.")

    # Wait for all threads to finish (this will block the main thread)
    # Keep main thread alive until all threads complete
    if node.thread:
        node.thread.join()
    for conn in node.connections.values():
        if conn.thread:
            conn.thread.join()
