# Kinesis Python

The [official Kinesis python library](https://github.com/awslabs/amazon-kinesis-client-python) requires use to use
Amazon's "MultiLangDaemon", which is a Java executable that operates by piping messages over STDIN/STDOUT.

ಠ_ಠ

While the desire to have a single implementation of the client library from a maintenance standpoint makes sense for
the team responsible for the KPL, requiring the JRE to be installed and having to account for the overhead of the
stream being consumed by Java and Python is not desireable for teams working in environments without Java.

This is a pure-Python implementation of Kinesis producer and consumer classes that leverages Python's multiprocessing
module to spawn a process per shard and then sends the messages back to the main process via a Queue.  It only depends
on the boto3 library.


# Overview

All of the functionality is wrapped in two classes: `KinesisConsumer` and `KinesisProducer`

## Consumer

The consumer works by launching a process per shard in the stream and then implementing the Python iterator protocol.

```python
from kinesis.consumer import KinesisConsumer

consumer = KinesisConsumer(stream_name='my-stream')
for message in consumer:
    print "Received message: {0}".format(message)
```

Messages received from each of the shard processes are passed back to the main process through a Python Queue where
they are yielded for processing.  Messages are not strictly ordered, but this is a property of Kinesis and not this 
implementation.


## Producer

The producer works by launching a single background process for accumulation and publishing to the stream.

```python
from kinesis.producer import KinesisProducer

producer = KinesisProducer(stream_name='my-stream')
producer.put('Hello World from Python')
```

By default the accumulation buffer time is 500ms, or the max record size of 1Mb, whichever occurs first.  You can
change the buffer time when you instantiate the producer via the `buffer_time` kwarg, specified in seconds.  For
example, if your primary concern is budget and not performance you could accumulate over a 60 second duration.

```python
producer = KinesisProducer(stream_name='my-stream', buffer_time=60)
```

The background process takes precaution to ensure that any accumulated messages are flushed to the stream at
shutdown time through signal handlers and the python atexit module, but it is not fully durable and if you were to
send a `kill -9` to the producer process any accumulated messages would be lost.


# TODO

## Multi-node consumption

Currently consumption is implemented under the assumption that a single node is the only consumer of all shards in a
stream.  For the workload that was used for the initial implementation this is not a problem, but if you had a stream
that consisted of many shards (10+) all of which were running close to their maximum throughput you would likely want
to spread your consumption out over multiple nodes, with each node consuming a subset of the shards.

This could be easily accomplished using the strong consistency of Dynamo as a method of coordination.

Contributions welcome.
