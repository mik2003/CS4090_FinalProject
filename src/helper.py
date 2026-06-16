def parse_message(message: str) -> tuple[str, int, str]:
    """Parse a message of the form "COMMAND NODE ARGUMENT" into a tuple (COMMAND, NODE, ARGUMENT).
    If the message does not contain a space, the argument is returned as an empty string.
    """

    args = message.split(" ")

    command, node, argument = (
        args[0],
        args[1],
        " ".join(args[2:]) if len(args) > 2 else "",
    )

    command = command.strip().upper()
    node = int(node.strip())
    argument = argument.strip()

    return command, node, argument
