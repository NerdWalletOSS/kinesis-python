# Kinesis Python

The [official Kinesis python library](https://github.com/awslabs/amazon-kinesis-client-python) requires use to use
Amazon's "MultiLangDaemon", which is a Java executable that operates by piping messages over STDIN/STDOUT.

ಠ_ಠ

This is a pure-Python implementation of Kinesis producer and consumer classes that leverages Python's multiprocessing
module to spawn a process per shard and then sends the messages back to the main process via a Queue.

It was started during the 2017-Q1 (really in Q2) Hackathon as part of the OPS-Bus work.
