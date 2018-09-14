import collections
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
import six

from offspring.process import SubprocessLoop

log = logging.getLogger(__name__)


def sizeof(obj, seen=None):
    """Recursively and fully calculate the size of an object"""
    obj_id = id(obj)
    try:
        if obj_id in seen:
            return 0
    except TypeError:
        seen = set()

    seen.add(obj_id)

    size = sys.getsizeof(obj)

    # since strings are iterabes we return their size explicitly first
    if isinstance(obj, six.string_types):
        return size
    elif isinstance(obj, collections.Mapping):
        return size + sum(
            sizeof(key, seen) + sizeof(val, seen)
            for key, val in six.iteritems(obj)
        )
    elif isinstance(obj, collections.Iterable):
        return size + sum(
            sizeof(item, seen)
            for item in obj
        )

    return size


class AsyncProducer(SubprocessLoop):
    """Async accumulator and producer based on a multiprocessing Queue"""
    # Tell our subprocess loop that we don't want to terminate on shutdown since we want to drain our queue first
    # TERMINATE_ON_SHUTDOWN = False
    TERMINATE_ON_SHUTDOWN = True
    WAIT_FOR_CHILD = True

    # Max size & count
    # Per: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
    #
    # * The maximum size of a data blob (the data payload before base64-encoding) is up to 1 MB.
    # * Each shard can support up to 1,000 records per second for writes, up to a maximum total data write rate of 1 MB
    #   per second (including partition keys).
    MAX_SIZE = (2 ** 20)
    MAX_COUNT = 1000

    def __init__(self, stream_name, buffer_time, queue, max_count=None, max_size=None, boto3_session=None):
        self.stream_name = stream_name
        self.buffer_time = buffer_time
        self.queue = queue
        self.records = []
        self.next_records = []
        self.alive = True
        self.max_count = max_count or self.MAX_COUNT
        self.max_size = max_size or self.MAX_SIZE
        self.terminate = False

        if boto3_session is None:
            boto3_session = boto3.Session()
        self.client = boto3_session.client('kinesis')

        self.start()

    def loop(self):
        records_size = 0
        records_count = 0
        timer_start = time.time()
        loop_status = 0

        while self.alive and (time.time() - timer_start) < self.buffer_time:
            # we want our queue to block up until the end of this buffer cycle, so we set out timeout to the amount
            # remaining in buffer_time by substracting how long we spent so far during this cycle
            queue_timeout = self.buffer_time - (time.time() - timer_start)
            try:
                log.debug("Fetching from queue with timeout: %s", queue_timeout)
                data, explicit_hash_key, partition_key = self.queue.get(block=True, timeout=queue_timeout)
            except Queue.Empty:
                continue
            except EOFError:
                continue
            except UnicodeDecodeError as exc:
                # log.exception("UnicodeDecodeError Exception: {0}".format(exc))
                log.error("UnicodeDecodeError Exception: {0}".format(exc))
                loop_status = False
                break
            except Exception as exc:
                log.exception("UNHANDLED EXCEPTION {0}".format(exc))
                log.error("Shutting down...")
                # self.alive = False
                loop_status = False
                break
            else:
                record = {
                    'Data': data,
                    'PartitionKey': partition_key or '{0}{1}'.format(time.clock(), time.time()),
                }
                if explicit_hash_key is not None:
                    record['ExplicitHashKey'] = explicit_hash_key

                # Get the size of any leftover records
                for i in self.records:
                    records_size += sizeof(i)
                    records_count += 1

                records_size += sizeof(record)
                if records_size >= self.max_size:
                    log.debug("Records exceed MAX_SIZE (%s)!  Adding to next_records: %s", self.max_size, record)
                    self.next_records = [record]
                    break

                log.debug("Adding to records (%d bytes): %s", records_size, record)
                self.records.append(record)

                records_count += 1
                if records_count == self.max_count:
                    log.debug("Records have reached MAX_COUNT (%s)!  Flushing records.", self.max_count)
                    break
        self.flush_records()
        return loop_status

    def end(self):
        # At the end of our loop (before we exit, i.e. via a signal) we change our buffer time to 250ms and then re-call
        # the loop() method to ensure that we've drained any remaining items from our queue before we exit.
        log.debug("Ending producer...".format(len(self.records)))
        while len(self.records) > 0:
            log.debug("Ending producer, flushing {0} records...".format(len(self.records)))
            self.buffer_time = 0.25
            self.loop()
        self.alive = False

    def flush_records(self):
        if self.records:
            log.debug("Flushing %d records", len(self.records))
            self.client.put_records(
                StreamName=self.stream_name,
                Records=self.records
            )
            log.debug("Flushed %d records", len(self.records))
            log.debug("%d next records", len(self.next_records))
        self.records = self.next_records
        self.next_records = []


class KinesisProducer(object):
    """Produce to Kinesis streams via an AsyncProducer"""
    def __init__(self, stream_name, buffer_time=0.5, max_count=None, max_size=None, boto3_session=None,
                 max_queue_size=None):
        self.max_queue_size = max_queue_size
        if self.max_queue_size:
            self.queue = multiprocessing.Queue(maxsize=self.max_queue_size)
        else:
            self.queue = multiprocessing.Queue()

        self.async_producer = AsyncProducer(stream_name, buffer_time, self.queue, max_count=max_count,
                                            max_size=max_size, boto3_session=boto3_session)

    def put(self, data, explicit_hash_key=None, partition_key=None):
        if not self.async_producer.alive:
            self.async_producer.shutdown()
            self.__init__()
        try:
            self.queue.put((data, explicit_hash_key, partition_key))
        except Exception as exc:
            log.exception("UNHANDLED EXCEPTION {0}".format(exc))
            log.error("Shutting down...")
            self.shutdown()
            raise

    def shutdown(self):
        log.debug("Shutting down the producer...")
        self.async_producer.shutdown()
