import logging
import multiprocessing
try:
    import Queue
except ImportError:
    # Python 3
    import queue as Queue
import sys
import time

import boto3

from offspring.process import SubprocessLoop

log = logging.getLogger(__name__)


class AsyncProducer(SubprocessLoop):
    """Async accumulator and producer based on a multiprocessing Queue"""
    # Tell our subprocess loop that we don't want to terminate on shutdown since we want to drain our queue first
    TERMINATE_ON_SHUTDOWN = False

    # This it the max size data that we'll send in a single call.  We use 99% of 1Mb to account for the extra overhead
    # of JSON syntax that is not taken into account when we use sys.getsizeof since it's measuring the python object
    MAX_SIZE = int((2 ** 20) * .99)

    # This is the max number of messages that we'll send in a single call.
    MAX_COUNT = 1000

    def __init__(self, stream_name, buffer_time, queue, boto3_session=None):
        self.stream_name = stream_name
        self.buffer_time = buffer_time
        self.queue = queue
        self.records = []
        self.next_records = []
        self.alive = True

        if boto3_session is None:
            boto3_session = boto3.Session()
        self.client = boto3_session.client('kinesis')

        self.start()

    def loop(self):
        records_size = 0
        records_count = 0
        timer_start = time.time()

        while self.alive and (time.time() - timer_start) < self.buffer_time:
            # we want our queue to block up until the end of this buffer cycle, so we set out timeout to the amount
            # remaining in buffer_time by substracting how long we spent so far during this cycle
            queue_timeout = self.buffer_time - (time.time() - timer_start)
            try:
                log.debug("Fetching from queue with timeout: %s", queue_timeout)
                data = self.queue.get(block=True, timeout=queue_timeout)
            except Queue.Empty:
                continue

            record = {
                'Data': data,
                'PartitionKey': '{0}{1}'.format(time.clock(), time.time()),
            }

            records_size += sys.getsizeof(record)
            if records_size >= self.MAX_SIZE:
                log.debug("Records exceed MAX_SIZE!  Adding to next_records: %s", record)
                self.next_records = [record]
                break

            log.debug("Adding to records (%d bytes): %s", records_size, record)
            self.records.append(record)

            records_count += 1
            if records_count == self.MAX_COUNT:
                log.debug("Records have reached MAX_COUNT!  Flushing records.")
                break

        self.flush_records()

    def end(self):
        # At the end of our loop (before we exit, i.e. via a signal) we change our buffer time to 250ms and then re-call
        # the loop() method to ensure that we've drained any remaining items from our queue before we exit.
        self.buffer_time = 0.25
        self.loop()

    def flush_records(self):
        if self.records:
            log.debug("Flushing %d records", len(self.records))
            self.client.put_records(
                StreamName=self.stream_name,
                Records=self.records
            )

        self.records = self.next_records
        self.next_records = []


class KinesisProducer(object):
    """Produce to Kinesis streams via an AsyncProducer"""
    def __init__(self, stream_name, buffer_time=1.0, boto3_session=None):
        self.queue = multiprocessing.Queue()
        self.async_producer = AsyncProducer(stream_name, buffer_time, self.queue, boto3_session=boto3_session)

    def put(self, data):
        self.queue.put(data)
