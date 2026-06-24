# Anonymous Quantum Key Distribution

To run the code, first start the SimulaQron backend. The number parameter specifies the number of nodes of the network. Here we choose four.

```bash
./start.sh 4
```

To start a node, the syntax is the following:

```bash
python anonymous_qkd.py <node_index> <role> <LAST?>
```

- `node_index`: zero-based index of the node.
- `role`: role of the Node. Can be either S(Sender), R(Receiver), or N(Node).
- `LAST`: specify this parameter if it's the last node.

For example, you can start the program by running the following commands, each in a separate terminal. Make sure to run them in this exact order.

```bash
python anonymous_qkd.py 0 N
python anonymous_qkd.py 1 S
python anonymous_qkd.py 2 N
python anonymous_qkd.py 3 R LAST
```

Alternatively, you can just run:

```bash
./run.sh
```

Which will execute a configuration specified in the script, and write the results to different files.
