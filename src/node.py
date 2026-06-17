import asyncio
import threading

from simulaqron.general.host_config import SocketsConfig
from simulaqron.sdk.protocol import (
    Callable,
    Awaitable,
    SimulaQronClassicalClient,
    SimulaQronClassicalServer,
    StreamReader,
    StreamWriter,
)
from simulaqron.settings import network_config, simulaqron_settings
from simulaqron.settings.network_config import Any, NodeConfigType

from netqasm.sdk.external import NetQASMConnection
from netqasm.sdk import EPRSocket


class NodeConnection:
    def __init__(self, node_index: int, target_index: int):
        self.node_index = node_index
        self.target_index = target_index
        self.epr_socket = EPRSocket(f"Node{self.target_index}")
        self.thread: None | threading.Thread = None
        self.classical_queue: list[tuple[str, str]] = (
            []
        )  # List of (command, data) tuples to send to the target node

    def start_classical_client(
        self,
        sockets_config: SocketsConfig,
        event_loop: Callable[[StreamReader, StreamWriter], Awaitable[None]],
    ) -> None:
        client = SimulaQronClassicalClient(sockets_config)

        async def _write_loop(reader: StreamReader, writer: StreamWriter) -> None:
            while True:
                # Check if there are any messages to send to the target node
                if self.classical_queue:
                    command, data = self.classical_queue.pop(0)
                    message = f"{command} {self.node_index} {data}\n"
                    writer.write(message.encode())
                    await writer.drain()
                else:
                    await asyncio.sleep(0)  # yield to event loop

        async def _client_loop(reader: StreamReader, writer: StreamWriter) -> None:
            # Combine the two loops. Use different tasks for the event loop and the write loop to run them concurrently
            write_task = asyncio.create_task(_write_loop(reader, writer))
            event_loop_task = asyncio.create_task(event_loop(reader, writer))  # type: ignore

            await asyncio.gather(write_task, event_loop_task)

        # Spawn the client connection in a separate thread to avoid blocking the main thread
        self.thread = threading.Thread(
            target=client.run_client,
            args=(f"Node{self.target_index}", _client_loop),
        )
        self.thread.start()

    def send_classical(self, command: str, data: str) -> None:
        # Implementation to send classical data to the target node
        self.classical_queue.append((command, data))


class NetworkNode:
    def __init__(self, index: int):
        self.index = index
        self.connections: dict[int, NodeConnection] = {}
        self.quantum_connection: None | NetQASMConnection = None
        self.sockets_config = SocketsConfig(
            network_config, "default", NodeConfigType.APP
        )
        self.thread: None | threading.Thread = None

    def start_classical_server(
        self, event_loop: Callable[[StreamReader, StreamWriter], Awaitable[None]]
    ) -> None:
        server = SimulaQronClassicalServer(self.sockets_config, f"Node{self.index}")
        server.register_client_handler(event_loop)

        # Spawn the thread in a separate thread
        self.thread = threading.Thread(target=server.start_serving)
        self.thread.start()

    def connect_to_node(
        self,
        target_index: int,
        event_loop: Callable[[StreamReader, StreamWriter], Awaitable[None]],
    ) -> None:
        connection = NodeConnection(self.index, target_index)
        connection.start_classical_client(self.sockets_config, event_loop)
        self.connections[target_index] = connection

    def complete_network(self) -> None:
        # Create the quantum interface using all EPR sockets
        self.quantum_connection = NetQASMConnection(
            f"Node{self.index}",
            epr_sockets=[conn.epr_socket for conn in self.connections.values()],
        )
