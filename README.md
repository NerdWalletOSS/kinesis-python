# Kinesis Python

The [official Kinesis python library](https://github.com/awslabs/amazon-kinesis-client-python) requires the use of
Amazon's "MultiLangDaemon", which is a Java executable that operates by piping messages over STDIN/STDOUT.

ಠ\_ಠ

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


### Locking, Checkpointing & Multi-instance consumption

When deploying an application with multiple instances DynamoDB can be leveraged as a way to coordinate which instance
is responsible for which shard, as it is not desirable to have each instance process all records.

A "state" backend that leverages DynamoDB allows consumers to coordinate which node is responsible which shards and
where in the stream we are currently reading from.

```python
from kinesis.consumer import KinesisConsumer
from kinesis.state import DynamoDB

consumer = KinesisConsumer(stream_name='my-stream', state=DynamoDB(table_name='my-kinesis-state'))
for message in consumer:
    print "Received message: {0}".format(message)
```


## Producer

The producer works by launching a single process for accumulation and publishing to the stream.

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
