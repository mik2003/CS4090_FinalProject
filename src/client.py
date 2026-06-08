import asyncio
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from simulaqron.general.host_config import SocketsConfig
from simulaqron.sdk.protocol import SimulaQronClassicalServer
from simulaqron.settings import network_config
from simulaqron.settings.network_config import NodeConfigType

from helper import is_valid_username


STATE_WAITING_LOGIN = "WAITING_LOGIN"
STATE_WAITING_HI = "WAITING_HI"
STATE_DONE = "DONE"

async def handle_login(writer: StreamWriter) -> str:
    global username

    while True:
        username = await asyncio.to_thread(
            input, "Bob — enter your 3-bit input (e.g. 000): "
        )
        username = username.strip()
        if is_valid_username(username):
            break
        print("Invalid username. Please enter alphanumeric username (including '-' and '_') between 3 and 32 characters", flush=True)

    writer.write(f"{username}\n".encode())

    return STATE_WAITING_HI

async def handle_hi(_writer: StreamWriter) -> str:
    print("Client: received HI", flush=True)
    return STATE_DONE

CLIENT_DISPATCH = {
    (STATE_WAITING_LOGIN, "LOGIN"): handle_login,
    (STATE_WAITING_HI, "HI"): handle_hi,
}

async def run_client(reader: StreamReader, writer: StreamWriter) -> None:
    state = STATE_WAITING_LOGIN
    while state != STATE_DONE:
        data = await reader.readline()
        if not data:
            print(f"Client [{state}]: connection dropped unexpectedly", flush=True)
            break
        msg = data.decode()
        print(f"Client [{state}]: received '{msg}'", flush=True)

        handler = CLIENT_DISPATCH.get((state, msg))

        if handler is None:
            print(f"Client [{state}]: no transition for '{msg}' - ignoring", flush=True)
            continue

        state = await handler(writer)

    print(f"Client: event loop finished (final state: {state})", flush=True)


if __name__ == "__main__":
    _here = Path(__file__).parent
    network_config.read_from_file(_here / "simulaqron_network.json")
    
    sockets_config = SocketsConfig(network_config, "default", NodeConfigType.APP)
    server = SimulaQronClassicalServer(sockets_config, "Client")
    server.register_client_handler(run_client)
    print("Client: starting server...", flush=True)
    server.start_serving()