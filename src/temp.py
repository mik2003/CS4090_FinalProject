import sys
import os
import asyncio
import threading
import numpy as np
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from netqasm.runtime.settings import set_simulator

set_simulator("simulaqron")

from netqasm.sdk import EPRSocket, Qubit

from simulaqron.settings import network_config, simulaqron_settings
from simulaqron.settings.network_config import NodeConfigType

# --- Classical key-distribution utilities (same lib alice.py / bob.py use) ----
# NOTE: adjust this path if lib/ does not sit at <project>/lib relative to here.
sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.kd_utils import (  # noqa: E402
    make_ldpc_matrix,
    compute_syndrome,
    decode_syndrome,
    compute_min_entropy_bsc,
    compute_min_entropy_after_leakage,
    compute_secure_key_length,
    generate_seed,
    privacy_amplify_with_seed,
)

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
CMD_STOP = "STOP"  # NEW: clean shutdown broadcast

# Procedures (what the last node dispatches after a GHZ merge)
PROC_ANON_TRANSMIT = "ANON_TRANSMIT"
PROC_ANON_ENTANGLEMENT = "ANON_ENTANGLEMENT"

# ── Pipeline parameters (identical on every node) ────────────────────────────
NUM_SYMBOLS = 32          # BB84 states to teleport; sifted key ~ NUM_SYMBOLS/2
D_V, D_C = 3, 10          # LDPC degrees (same as alice/bob); need sifted >= D_C
LDPC_SEED = 42            # must match across nodes
P_E = 0.4                 # Eve's assumed BSC flip prob (entropy bound)
EPSILON = 1e-3            # privacy-amplification security parameter
KEYLEN_BITS = 16          # width for serializing integers over the bit channel
HEADER_BITS = 2 * KEYLEN_BITS   # ec_len + pa_len

# Phases of the protocol
PHASE_BB84_DISTRIBUTION    = "BB84_DISTRIBUTION"      # noqa: E221
PHASE_RECON_SENDER_BASES   = "RECON_SENDER_BASES"     # sender announces theta_i
PHASE_RECON_RECEIVER_BASES = "RECON_RECEIVER_BASES"   # receiver announces phi_i
PHASE_HEADER               = "HEADER"                 # announce ec_len, pa_len  # noqa: E221
PHASE_EC_SYNDROME          = "EC_SYNDROME"            # sender broadcasts syndrome  # noqa: E221
PHASE_PA                   = "PA"                     # sender broadcasts PA params  # noqa: E221
PHASE_DONE                 = "DONE"                   # noqa: E221

# Substeps within one BB84 symbol during distribution
SUB_ENT  = "ENT"     # anonymous entanglement + teleport            # noqa: E221
SUB_TX_X = "TX_X"    # broadcast X-correction bit b
SUB_TX_Z = "TX_Z"    # broadcast Z-correction bit a, then R measures


# ── Shared mutable state across the event loop ───────────────────────────────
state = STATE_INIT
procedure = PROC_ANON_ENTANGLEMENT   # CHANGED: first round is an ENT round
phase = PHASE_BB84_DISTRIBUTION

q: Qubit | None = None
ancilla: Qubit | None = None
memory: Qubit | None = None   # holds the teleported half between TX rounds
rng = np.random.default_rng()

# ── Protocol schedule (advances in lockstep on every node) ───────────────────
symbol_index = 0
substep      = SUB_ENT
recon_index  = 0
header_index = 0
ec_index     = 0
pa_index     = 0
round_index  = 0       # only the last node uses this as a safety counter

ec_len_global = 0      # learned by ALL nodes from the HEADER broadcast
pa_len_global = 0

# ── Sender ("S") private data ──
sender_bits:      list[int] = []
sender_bases:     list[int] = []
pending_corr:     tuple[int, int] | None = None   # (b = X-corr, a = Z-corr)
recv_bases_seen:  list[int] = []   # phi_i learned during receiver-basis recon
ec_payload:       list[int] = []   # syndrome bits to broadcast
pa_payload:       list[int] = []   # KEYLEN_BITS(key_len) + seed bits
header_payload:   list[int] = []   # KEYLEN_BITS(ec_len) + KEYLEN_BITS(pa_len)

# ── Receiver ("R") private data ──
recv_bases:        list[int] = []
recv_outcomes:     list[int] = []
sender_bases_seen: list[int] = []  # theta_i learned during sender-basis recon
ec_collected:      list[int] = []
pa_collected:      list[int] = []

# ── Shared / outputs ──
header_collected: list[int] = []   # every node records the public header bits
raw_key = None                     # x_A on sender, x_B on receiver
final_key = None
_shutting_down = False

# Node info
node = NetworkNode(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
role = (
    sys.argv[2].strip().upper() if len(sys.argv) > 2 else "N"
)  # Sender(S), Receiver(R), or Node(N)
is_first_node = node.index == 0
is_last_node = sys.argv[3].strip().upper() == "LAST" if len(sys.argv) > 3 else False

if role not in ["S", "R", "N"]:
    raise ValueError("Invalid role. Must be 'S', 'R', or 'N'.")


def log(message: str) -> None:
    print(
        f"Node({node.index}) [{role}] [{state}] [{phase}/{substep}]: {message}",
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


# ---- Schedule helpers ---------------------------------------------


def int_to_bits(value: int, width: int) -> list[int]:
    return [(value >> (width - 1 - i)) & 1 for i in range(width)]


def bits_to_int(bits: list[int]) -> int:
    v = 0
    for b in bits:
        v = (v << 1) | int(b)
    return v


def current_round_is_entanglement() -> bool:
    return phase == PHASE_BB84_DISTRIBUTION and substep == SUB_ENT


def current_transmitter() -> str:
    """Which role ENCODES during the current anonymous-transmission round."""
    if phase == PHASE_RECON_RECEIVER_BASES:
        return "R"
    return "S"   # corrections, sender bases, header, syndrome, PA


def next_outgoing_bit() -> int:
    """The single bit the transmitter encodes in the current ANON_TRANSMIT lap."""
    if phase == PHASE_BB84_DISTRIBUTION:
        assert pending_corr is not None
        return pending_corr[0] if substep == SUB_TX_X else pending_corr[1]
    if phase == PHASE_RECON_SENDER_BASES:
        return sender_bases[recon_index]
    if phase == PHASE_RECON_RECEIVER_BASES:
        return recv_bases[recon_index]
    if phase == PHASE_HEADER:
        return header_payload[header_index]
    if phase == PHASE_EC_SYNDROME:
        return ec_payload[ec_index]
    if phase == PHASE_PA:
        return pa_payload[pa_index]
    return 0


# ---- Event handlers (network setup -- UNCHANGED) ------------------


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
    if is_last_node:
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
        q = next_node_socket().create_keep()[0]
        node.quantum_connection.flush()
        send_to_next_node(CMD_RECV_EPR)
    elif is_last_node:
        q = prev_node_socket().recv_keep()[0]
        node.quantum_connection.flush()
        log("Created all EPRs pairs. Starting merge...")
        send_to_next_node(CMD_MERGE_EPR)
    else:
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
        send_to_next_node(CMD_MERGE_EPR)
        return STATE_EPR_MERGED

    correction = int(argument) if argument != "" else 0
    if correction == 1:
        q.X()

    if is_last_node:
        log("GHZ state completed.")
        if procedure == PROC_ANON_ENTANGLEMENT:
            send_to_next_node(CMD_ANON_ENTANGLEMENT)
        else:
            send_to_next_node(CMD_ANON_TRANSMIT)
        return STATE_EPR_MERGED

    if ancilla is None:
        raise ValueError("Ancilla qubit not initialized")

    q.cnot(ancilla)
    m = ancilla.measure()
    node.quantum_connection.flush()
    m = int(m)

    send_to_next_node(CMD_MERGE_EPR, f"{m}")
    return STATE_EPR_MERGED


# ---- Anonymous primitives (REPLACED) ------------------------------


async def handle_anon_transmit_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    # GHZ-based anonymous broadcast of ONE bit from `current_transmitter()`.
    global q

    if q is None:
        raise ValueError("Qubit not initialized")
    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    parity = int(argument) if argument != "" else 0

    # Only the designated transmitter encodes (sender for most phases,
    # receiver while it announces its bases).
    if role == current_transmitter() and next_outgoing_bit():
        q.Z()

    q.H()
    m = q.measure()
    node.quantum_connection.flush()
    parity ^= int(m)

    if is_last_node:
        send_to_next_node(CMD_TRANSMIT_RESULT, f"{parity}")
    else:
        send_to_next_node(CMD_ANON_TRANSMIT, f"{parity}")

    return STATE_TRANSMIT_DONE


async def handle_anon_entanglement_broadcast(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    # Establish anonymous entanglement: plain nodes measure in X and disentangle;
    # the sender masks with a random Z^b; the receiver KEEPS its qubit and applies
    # the Z correction later (in handle_entanglement_completed) on the full parity.
    global q

    if q is None:
        raise ValueError("Qubit not initialized")
    if node.quantum_connection is None:
        raise ValueError("Quantum connection not initialized")

    parity = int(argument) if argument != "" else 0

    if role == "N":
        q.H()
        m = q.measure()
        node.quantum_connection.flush()
        parity ^= int(m)
    elif role == "S":
        b = int(rng.integers(0, 2))   # random masking bit hides the sender
        if b:
            q.Z()
        parity ^= b
        # S keeps q (its Bell half) -- no measurement.
    # role == "R": keep q untouched; correction happens on the completion lap.

    if is_last_node:
        send_to_next_node(CMD_ENTANGLEMENT_COMPLETED, f"{parity}")
    else:
        send_to_next_node(CMD_ANON_ENTANGLEMENT, f"{parity}")

    return STATE_ENTANGLEMENT_DONE


# ---- Result handlers (FILLED IN) ----------------------------------


async def handle_transmit_result(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global memory

    result = int(argument)

    # ---- role action for the CURRENT round (before advancing) ----
    if phase == PHASE_BB84_DISTRIBUTION and role == "R":
        if substep == SUB_TX_X:
            if result and memory is not None:
                memory.X()                  # X^b
                node.quantum_connection.flush()
        elif substep == SUB_TX_Z and memory is not None:
            if result:
                memory.Z()                  # Z^a
            phi = int(rng.integers(0, 2))   # receiver's random measurement basis
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

    elif phase == PHASE_HEADER:
        header_collected.append(result)     # EVERY node records (lengths public)

    elif phase == PHASE_EC_SYNDROME and role == "R":
        ec_collected.append(result)

    elif phase == PHASE_PA and role == "R":
        pa_collected.append(result)

    # ---- propagate or consume ----
    if is_last_node:
        advance_schedule()
        conductor_next()
    else:
        send_to_next_node(CMD_TRANSMIT_RESULT, f"{result}")
        advance_schedule()

    return STATE_READY


async def handle_entanglement_completed(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    global q, memory, pending_corr

    final_parity = int(argument) if argument != "" else 0

    if role == "R":
        if final_parity:
            q.Z()                       # FIX: Z (not X), on the COMPLETE parity
        node.quantum_connection.flush()
        memory = q                      # hold across the next two TX rounds
        q = None
        log("Stored anonymous Bell half; awaiting teleport corrections")

    elif role == "S":
        # Prepare a random BB84 state |psi> and teleport it through `q`.
        v = int(rng.integers(0, 2))         # bit value
        theta = int(rng.integers(0, 2))     # 0 = Z basis, 1 = X basis
        psi = Qubit(node.quantum_connection)    # CHECK: fresh-qubit allocation
        if v:
            psi.X()
        if theta:
            psi.H()
        psi.cnot(q)        # control = data qubit, target = our Bell half
        psi.H()
        a_f = psi.measure()    # Z-correction bit (data qubit after H)
        b_f = q.measure()      # X-correction bit (our Bell half)
        node.quantum_connection.flush()    # measure both, THEN read as ints
        a, b = int(a_f), int(b_f)
        q = None
        sender_bits.append(v)
        sender_bases.append(theta)
        pending_corr = (b, a)               # broadcast b (X) first, then a (Z)
        log(f"Teleported BB84 (v={v}, basis={theta}); corr X:{b} Z:{a}")

    # ---- propagate or consume ----
    if is_last_node:
        advance_schedule()
        conductor_next()
    else:
        send_to_next_node(CMD_ENTANGLEMENT_COMPLETED, f"{final_parity}")
        advance_schedule()

    return STATE_READY


# ---- Schedule advance (runs on EVERY node, once per round) ---------


def advance_schedule() -> None:
    global phase, symbol_index, substep, recon_index, header_index
    global ec_index, pa_index, ec_len_global, pa_len_global

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
            _sift_and_prepare()
            phase = PHASE_HEADER
            header_index = 0

    elif phase == PHASE_HEADER:
        header_index += 1
        if header_index >= HEADER_BITS:
            ec_len_global = bits_to_int(header_collected[0:KEYLEN_BITS])
            pa_len_global = bits_to_int(header_collected[KEYLEN_BITS:HEADER_BITS])
            if ec_len_global > 0:
                phase, ec_index = PHASE_EC_SYNDROME, 0
            elif pa_len_global > 0:
                phase, pa_index = PHASE_PA, 0
            else:
                _finish_pa()
                phase = PHASE_DONE

    elif phase == PHASE_EC_SYNDROME:
        ec_index += 1
        if ec_index >= ec_len_global:
            if pa_len_global > 0:
                phase, pa_index = PHASE_PA, 0
            else:
                _finish_pa()
                phase = PHASE_DONE

    elif phase == PHASE_PA:
        pa_index += 1
        if pa_index >= pa_len_global:
            _finish_pa()
            phase = PHASE_DONE


# ---- Conductor (last node only): start next round or stop ---------


def conductor_next() -> None:
    global procedure
    if phase == PHASE_DONE:
        log("Protocol complete -- broadcasting STOP")
        send_to_next_node(CMD_STOP)
        _schedule_shutdown()
        return
    procedure = (
        PROC_ANON_ENTANGLEMENT if current_round_is_entanglement() else PROC_ANON_TRANSMIT
    )
    send_to_next_node(CMD_RECV_EPR)   # rebuild a fresh GHZ for the next round


# ---- Classical back-half (sift + LDPC reconciliation + Toeplitz PA) ----


def _kept_indices(bases_a: list[int], bases_b: list[int]) -> list[int]:
    return [i for i in range(NUM_SYMBOLS) if bases_a[i] == bases_b[i]]


def _truncate_to_block(bits: list[int]) -> np.ndarray:
    n = (len(bits) // D_C) * D_C        # LDPC needs length divisible by D_C
    return np.array(bits[:n], dtype=np.uint8)


def n_syndrome_len(n: int) -> int:
    return D_V * (n // D_C)


def _sift_and_prepare() -> None:
    """Runs once on every node at the end of receiver-basis reconciliation.
    Sender builds the syndrome / PA / header payloads; receiver builds x_B."""
    global raw_key, ec_payload, pa_payload, header_payload, final_key

    if role == "S":
        kept = _kept_indices(sender_bases, recv_bases_seen)
        x_A = _truncate_to_block([sender_bits[i] for i in kept])
        raw_key = x_A
        n = len(x_A)
        log(f"Sender sifted {len(kept)} bits -> using n={n}")

        if n == 0:
            log("WARNING: sifted key shorter than one LDPC block. "
                "Increase NUM_SYMBOLS. Skipping EC/PA.")
            ec_payload, pa_payload = [], []
            header_payload = int_to_bits(0, KEYLEN_BITS) + int_to_bits(0, KEYLEN_BITS)
            final_key = np.zeros(0, dtype=np.uint8)
            return

        H = make_ldpc_matrix(n, D_V, D_C, LDPC_SEED)
        syndrome = compute_syndrome(H, x_A)
        ec_payload = [int(b) for b in syndrome]

        h_min = compute_min_entropy_bsc(n, 1 - P_E)
        leaked = n_syndrome_len(n)
        h_after = compute_min_entropy_after_leakage(h_min, leaked)
        key_len = max(0, int(compute_secure_key_length(h_after, EPSILON)))
        log(f"Sender H_min={h_min:.2f}, after leak={h_after:.2f}, key_len={key_len}")

        if key_len > 0:
            seed = generate_seed(n, key_len, rng)
            final_key = privacy_amplify_with_seed(x_A, key_len, seed)
        else:
            log("WARNING: no secure key at this length "
                "(raise NUM_SYMBOLS, lower P_E, or raise EPSILON).")
            seed = np.zeros(0, dtype=np.uint8)
            final_key = np.zeros(0, dtype=np.uint8)

        pa_payload = int_to_bits(key_len, KEYLEN_BITS) + [int(b) for b in seed]
        header_payload = (
            int_to_bits(len(ec_payload), KEYLEN_BITS)
            + int_to_bits(len(pa_payload), KEYLEN_BITS)
        )
        log("final key " + np.array2string(np.asarray(final_key), max_line_width=10000))

    elif role == "R":
        kept = _kept_indices(sender_bases_seen, recv_bases)
        x_B = _truncate_to_block([recv_outcomes[i] for i in kept])
        raw_key = x_B
        log(f"Receiver sifted {len(kept)} bits -> using n={len(x_B)}")


def _finish_pa() -> None:
    """Runs once at the end of the PA phase. Receiver decodes + amplifies."""
    global final_key

    if role != "R":
        return
    if raw_key is None or len(raw_key) == 0 or not pa_collected:
        final_key = np.zeros(0, dtype=np.uint8)
        return

    x_B = np.asarray(raw_key, dtype=np.uint8)
    n = len(x_B)

    # syndrome reconciliation (bob.py handle_syndrome)
    s_alice = np.array(ec_collected[:n_syndrome_len(n)], dtype=np.uint8)
    H = make_ldpc_matrix(n, D_V, D_C, LDPC_SEED)
    s_bob = compute_syndrome(H, x_B)
    s_err = np.logical_xor(s_alice, s_bob).astype(np.uint8)
    err = decode_syndrome(H, s_err, P_E)        # CHECK: BP error-rate argument
    x_A_hat = np.logical_xor(x_B, err).astype(np.uint8)
    log(f"Receiver corrected {int(np.sum(x_B != x_A_hat))} bits")

    # privacy amplification (bob.py handle_pa)
    key_len = bits_to_int(pa_collected[:KEYLEN_BITS])
    seed = np.array(pa_collected[KEYLEN_BITS:], dtype=np.uint8)
    if key_len > 0:
        final_key = privacy_amplify_with_seed(x_A_hat, key_len, seed)
    else:
        final_key = np.zeros(0, dtype=np.uint8)
    log("final key " + np.array2string(np.asarray(final_key), max_line_width=10000))


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


async def handle_stop(
    writer: StreamWriter, conn_node_index: int, argument: str
) -> str:
    log("Received STOP -- shutting down")
    if role in ("S", "R") and final_key is not None:
        log(f"FINAL KEY ({len(final_key)} bits): "
            + np.array2string(np.asarray(final_key), max_line_width=10000))
    if not is_last_node:
        send_to_next_node(CMD_STOP)
    _schedule_shutdown()
    return STATE_READY


# ---- Communication (UNCHANGED) ------------------------------------


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
    (STATE_INIT, CMD_START): handle_start_broadcast,
    (STATE_READY, CMD_RECV_EPR): handle_recv_epr_broadcast,
    (STATE_INIT_GHZ, CMD_MERGE_EPR): handle_epr_merge_broadcast,
    (STATE_EPR_MERGED, CMD_ANON_TRANSMIT): handle_anon_transmit_broadcast,
    (STATE_EPR_MERGED, CMD_ANON_ENTANGLEMENT): handle_anon_entanglement_broadcast,
    (STATE_TRANSMIT_DONE, CMD_TRANSMIT_RESULT): handle_transmit_result,
    (STATE_ENTANGLEMENT_DONE, CMD_ENTANGLEMENT_COMPLETED): handle_entanglement_completed,
    (STATE_READY, CMD_STOP): handle_stop,   # NEW
}


# ---- Event loop ---------------------------------------------------


async def event_loop(reader: StreamReader, writer: StreamWriter) -> None:
    global state

    while True:
        data = await reader.readline()
        if not data:               # EOF: peer closed the connection
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
            return

        state = await handler(writer, conn_node_index, argument)

        log(f"Transitioning to state {state}")


# ---- Main (UNCHANGED) ---------------------------------------------

if __name__ == "__main__":
    _here = Path(__file__).parent
    simulaqron_settings.read_from_file(_here / "simulaqron_settings.json")
    network_config.read_from_file(_here / "simulaqron_network.json")

    node.start_classical_server(event_loop)

    if node.index > 0:
        log(f"Connecting to previous node (Node{node.index - 1})...")
        node.connect_to_node(node.index - 1, event_loop)
        asyncio.run(asyncio.sleep(1))
        send_to_prev_node(CMD_NEW_NODE)

    if is_last_node:
        log("Connecting to next node (Node0) to complete the ring...")
        node.connect_to_node(0, event_loop)
        node.complete_network()
        asyncio.run(asyncio.sleep(1))
        send_to_next_node(CMD_NEW_NODE)
        send_to_next_node(CMD_START)
        log("Completed network setup. This is the last node.")

    if node.thread:
        node.thread.join()
    for conn in node.connections.values():
        if conn.thread:
            conn.thread.join()