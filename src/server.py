"""Server"""

from asyncio import StreamReader, StreamWriter
from pathlib import Path

from simulaqron.general.host_config import SocketsConfig
from simulaqron.sdk.protocol import SimulaQronClassicalServer
from simulaqron.settings import network_config
from simulaqron.settings.network_config import NodeConfigType

from helper import is_valid_username

STATE_WAITING_LOGIN = "WAITING_LOGIN"
STATE_DONE = "DONE"


async def handle_login(writer: StreamWriter, username: str) -> str:
    if not is_valid_username(username):
        print(f"Server: Invalid username '{username}'", flush=True)
        return STATE_WAITING_LOGIN

    print(f"Server: '{username}' logged in", flush=True)
    writer.write(b"HI")
    print("Server: sent HI", flush=True)

    return STATE_DONE

SERVER_DISPATCH = {
    (STATE_WAITING_LOGIN, "LOGIN"): handle_login,
}

async def run_server(reader: StreamReader, writer: StreamWriter) -> None:

    state = STATE_WAITING_LOGIN

    while state != STATE_DONE:
        data = await reader.readline()
        if not data:
            print(f"Server [{state}]: connection dropped unexpectedly.")
            break
        raw_msg = data.decode()
        
        parts = raw_msg.strip().split(":")
        if len(parts) == 1:
            cmd = parts[0]
        elif len(parts) == 2:
            cmd, msg = parts

        handler = SERVER_DISPATCH.get((state, cmd))

        if handler is None:
            print(f"Server [{state}]: no transition for '{cmd}' - ignoring.")
            continue

        state = await handler(writer, msg)

    print(f"Server: event loop finished (final state: {state}).")


if __name__ == "__main__":
    _here = Path(__file__).parent
    network_config.read_from_file(_here / "simulaqron_network.json")
    
    sockets_config = SocketsConfig(network_config, "default", NodeConfigType.APP)
    server = SimulaQronClassicalServer(sockets_config, "Server")
    server.register_client_handler(run_server)
    print("Server: starting server...", flush=True)
    server.start_serving()
