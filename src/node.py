"""Client"""

import sys
import asyncio
import multiprocessing
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from simulaqron.general.host_config import SocketsConfig
from simulaqron.sdk.protocol import SimulaQronClassicalClient, SimulaQronClassicalServer
from simulaqron.settings import network_config, simulaqron_settings
from simulaqron.settings.network_config import NodeConfigType

from helper import *

# States
STATE_INIT = 0
STATE_READY = 1


def print_state(state_value: int) -> str:
    if state_value == STATE_INIT:
        return "INIT"
    elif state_value == STATE_READY:
        return "READY"
    else:
        return f"UNKNOWN({state_value})"


# Commands
CMD_NEW_NODE = "NEW_NODE"
CMD_START = "START"

# Shared state across processes
state = multiprocessing.Value("i", STATE_INIT)
role = "N"  # Sender(S), Receiver(R), or Node(N)
node_index = 0
is_last_node = False


def log(message: str) -> None:
    print(
        f"Node({node_index}) [{role}] [{print_state(state.value)}]: {message}",
        flush=True,
    )


# ---- Connections Manager ------------------------------------------

prev_node_queue: asyncio.Queue = asyncio.Queue()
next_node_queue: asyncio.Queue = asyncio.Queue()
queues = (prev_node_queue, next_node_queue)


def start_node_server() -> None:
    _here = Path(__file__).parent
    simulaqron_settings.read_from_file(_here / "simulaqron_settings.json")
    network_config.read_from_file(_here / "simulaqron_network.json")

    sockets_config = SocketsConfig(network_config, "default", NodeConfigType.APP)
    server = SimulaQronClassicalServer(sockets_config, f"Node{node_index}Server")
    server.register_client_handler(run_node_server)

    log(f"Starting server for Node{node_index}...")

    # Spawn the server in a separate process to allow the main thread to connect to other nodes
    server_process = multiprocessing.Process(target=server.start_serving)
    server_process.start()


def connect_to_node(conn_node_index: int, queues: tuple, queue_type: str) -> None:
    _here = Path(__file__).parent
    simulaqron_settings.read_from_file(_here / "simulaqron_settings.json")
    network_config.read_from_file(_here / "simulaqron_network.json")

    sockets_config = SocketsConfig(network_config, "default", NodeConfigType.APP)
    client = SimulaQronClassicalClient(sockets_config)

    log(f"Connecting to Node{conn_node_index}...")

    # Spawn the client connection in a separate thread to avoid blocking the main thread
    client_process = multiprocessing.Process(
        target=client.run_client,
        args=(f"Node{conn_node_index}Server", run_node_client, queues, queue_type),
    )
    client_process.start()


# ---- Initializer --------------------------------------------------


def initialize():
    global role, node_index, is_last_node

    node_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    # Get role from sys.argv: Sender(S), Receiver(R), or Node(N)
    if len(sys.argv) > 2:
        role = sys.argv[2].strip().upper()
    else:
        role = "N"

    if role not in {"S", "R", "N"}:
        log(f"Invalid role '{role}' provided via command line. Defaulting to 'N'.")
        role = "N"

    is_last_node = sys.argv[3].strip().upper() == "LAST" if len(sys.argv) > 3 else False


# ---- Event handlers -----------------------------------------------


async def handle_new_node(
    writer: StreamWriter, conn_node_index: int, argument: str, queues: tuple
) -> int:

    if conn_node_index == node_index + 1:
        next_node_writer = writer
        log(f"Connected to next node (Node{conn_node_index})")
    elif node_index == 0:  # Connecting first to last node
        prev_node_writer = writer
        log(f"Connected to previous node (Node{conn_node_index})")
    else:
        log(f"Connected to unexpected node index {conn_node_index} - ignoring")

    return STATE_INIT


async def handle_start(
    writer: StreamWriter, conn_node_index: int, argument: str, queues: tuple
) -> int:
    log("Received START command. Transitioning to READY state.")

    # Broadcast START across the network to ensure all nodes are ready
    await send_to_next_node(CMD_START)

    return STATE_READY


# ---- Main ---------------------------------------------------------


def _send_msg(writer: StreamWriter, command: str, argument: str = "") -> None:
    global node_index
    writer.write(f"{command} {node_index} {argument}\n".encode())


async def send_to_prev_node(command: str, argument: str = "") -> None:
    global prev_node_queue
    log(f"Sending '{command} {argument}' to previous node...")
    await prev_node_queue.put((command, argument))


async def send_to_next_node(command: str, argument: str = "") -> None:
    global next_node_queue
    log(f"Sending '{command} {argument}' to next node...")
    await next_node_queue.put((command, argument))


# ---- Handlers map -------------------------------------------------


EVENT_HANDLERS = {
    (STATE_INIT, CMD_NEW_NODE): handle_new_node,
    (STATE_READY, CMD_START): handle_start,
}


# ---- Event loop ---------------------------------------------------


async def event_loop(reader: StreamReader, writer: StreamWriter, queues: tuple) -> None:

    data = await reader.readline()
    if not data:
        raise ConnectionError("Connection closed by peer")
    msg = data.decode()

    log(f"Received message: '{msg.strip()}'")

    try:
        command, conn_node_index, argument = parse_message(msg)
    except ValueError as e:
        log(f"Error parsing message '{msg.strip()}': {e}")
        return

    handler = EVENT_HANDLERS.get((state.value, command))

    if handler is None:
        log(f"no transition for '{msg.strip()}' - ignoring")
        return

    state.value = await handler(writer, conn_node_index, argument, queues)


async def run_node_server(reader: StreamReader, writer: StreamWriter) -> None:

    while True:
        await event_loop(reader, writer, queues)


async def run_node_client(
    reader: StreamReader, writer: StreamWriter, queues: tuple, queue_type: str
) -> None:

    # When connecting to a node, send a NEW_NODE message to inform the other node of this connection
    _send_msg(writer, CMD_NEW_NODE)

    this_queue = prev_node_queue if queue_type == "prev" else next_node_queue

    while True:
        # Check queues for messages to send to the previous or next node
        if not this_queue.empty():
            command, argument = await this_queue.get()
            _send_msg(writer, command, argument)

        await event_loop(reader, writer, queues)


# ---- Main ---------------------------------------------------------

if __name__ == "__main__":
    initialize()

    start_node_server()

    if node_index > 0:
        connect_to_node(node_index - 1, queues, "prev")
    if is_last_node:
        connect_to_node(0, queues, "next")
        # Wait for a moment to ensure the last node is connected
        asyncio.run(asyncio.sleep(1))
        asyncio.run(send_to_next_node(CMD_START))
