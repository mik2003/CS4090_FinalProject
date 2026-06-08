from asyncio import StreamReader, StreamWriter
from pathlib import Path

from simulaqron.general.host_config import SocketsConfig
from simulaqron.sdk.protocol import SimulaQronClassicalClient
from simulaqron.settings import network_config
from simulaqron.settings.network_config import NodeConfigType

STATE_WAITING_LOGIN = "WAITING_LOGIN"
STATE_DONE = "DONE"


async def run_alice(reader: StreamReader, writer: StreamWriter) -> None:

    async def handle_login(writer: StreamWriter, msg: str) -> str:
        print("Alice: received HI", flush=True)
        writer.write(b"HI")
        print("Alice: sent HI", flush=True)
        return STATE_DONE

    dispatch = {
        (STATE_WAITING_LOGIN, "LOGIN"): handle_login,
    }

    state = STATE_WAITING_LOGIN

    while state != STATE_DONE:
        data = await reader.readline()
        if not data:
            print(f"Server [{state}]: connection dropped unexpectedly.")
            break
        msg = data.decode()

        handler = dispatch.get((state, msg))

        if handler is None:
            print(f"Server [{state}]: no transition for '{msg}' - ignoring.")
            continue

        state = await handler(writer)

    print(f"Server: event loop finished (final state: {state}).")


if __name__ == "__main__":
    _here = Path(__file__).parent
    network_config.read_from_file(_here / "simulaqron_network.json")

    sockets_config = SocketsConfig(network_config, "default", NodeConfigType.APP)
    client = SimulaQronClassicalClient(sockets_config)

    print("Server: running...")
    client.run_client("Bob", run_alice)
