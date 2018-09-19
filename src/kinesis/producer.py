import collections
import logging
from multiprocessing import Process, Queue, Manager
try:
    # Python 3
    import queue
except ImportError:
    import Queue as queue
import sys
import time

import boto3
import six

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


class KinesisProducer(object):
    """Produce to Kinesis streams via an AsyncProducer"""

    MAX_SIZE = (2 ** 20)
    MAX_COUNT = 1000

    def __init__(self, stream_name, buffer_time=0.5, max_count=None, max_size=None, boto3_session=None,
                 max_queue_size=None):
        self.stream_name = stream_name
        self.buffer_time = buffer_time
        self.max_count = max_count
        self.max_size = max_size
        self.boto3_session = boto3_session
        self.max_queue_size = max_queue_size
        self.max_queue_size = max_queue_size
        self.manager = Manager()
        if self.max_queue_size:
            self.queue = self.manager.Queue(maxsize=self.max_queue_size)
        else:
            self.queue = self.manager.Queue()
        self.records = []
        self.next_records = []
        self.alive = True
        self.max_count = max_count or self.MAX_COUNT
        self.max_size = max_size or self.MAX_SIZE
        self.async_producer = None

        if boto3_session is None:
            boto3_session = boto3.Session()
        self.client = boto3_session.client('kinesis')

    def start(self):
        log.debug("Starting producer...")
        self._setup_producer()

    def _setup_producer(self):
        # Don't do anything if we have a producer...
        if hasattr(self, "async_producer") and self.async_producer and self.async_producer.is_alive():
            return

        if hasattr(self, "async_producer"):
            del self.async_producer

        self.async_producer = Process(target=self._run_writer)
        self.async_producer.start()

    def _run_writer(self):
        while self.alive:
            records_size = 0
            records_count = 0
            timer_start = time.time()
            log.debug("{0} items in producer queue.".format(self.queue))
            try:
                while self.alive and (time.time() - timer_start) < self.buffer_time:
                    # we want our queue to block up until the end of this buffer cycle, so we set out timeout to the amount
                    # remaining in buffer_time by substracting how long we spent so far during this cycle
                    queue_timeout = self.buffer_time - (time.time() - timer_start)
                    try:
                        log.debug("Fetching from queue with timeout: %s", queue_timeout)
                        data, explicit_hash_key, partition_key = self.queue.get(block=True, timeout=queue_timeout)
                    except queue.Empty:
                        pass
                    except EOFError:
                        pass
                    except UnicodeDecodeError as exc:
                        # log.exception("UnicodeDecodeError Exception: {0}".format(exc))
                        log.error("UnicodeDecodeError Exception: {0}".format(exc))
                        pass
                    except Exception as exc:
                        log.exception("UNHANDLED EXCEPTION {0}".format(exc))
                        log.error("Shutting down...")
                        # self.alive = False
                        return False
                    else:
                        # record_queue.task_done()
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
                            # log.debug("Records exceed MAX_SIZE (%s)!  Adding to next_records: %s", self.max_size, record)
                            log.debug("Records exceed MAX_SIZE (%s)!", self.max_size)
                            self.next_records = [record]
                            break

                        # log.debug("Adding to records (%d bytes): %s", records_size, record)
                        log.debug("Adding to records (%d bytes)", records_size)
                        self.records.append(record)

                        records_count += 1
                        if records_count == self.max_count:
                            log.debug("Records have reached MAX_COUNT (%s)!  Flushing records.", self.max_count)
                            break
            except OSError as exc:
                log.error("OSError Exception: {0}".format(exc))
                # This is a memory problem, most likely.
                return False
            log.debug("Attempting to flush {0} records...".format(len(self.records)))
            self.flush_records()
        log.debug("Ending producer, flushing {0} records...".format(len(self.records)))
        self.flush_records()
        self.alive = False
        log.info("Producer Ended...")
        return False

    def put(self, data, explicit_hash_key=None, partition_key=None):
        if hasattr(self, "async_producer") and not self.async_producer.is_alive():
            # self.async_producer.shutdown()
            self._setup_producer()
        try:
            self.queue.put((data, explicit_hash_key, partition_key), block=True, timeout=0.25)
        except Queue.Full:
            if hasattr(self, "async_producer") and not self.async_producer.is_alive():
                # self.async_producer.shutdown()
                self._setup_producer()
        except Exception as exc:
            log.exception("UNHANDLED EXCEPTION {0}".format(exc))
            log.error("Shutting down...")
            self.shutdown()
            raise

    def shutdown(self):
        log.debug("Shutting down the producer...")
        max_retries = 30

        # If our process is dead, it's dead.
        if self.async_producer.is_alive() is False:
            log.debug("Producer process is dead.")
        else:
            log.debug("Waiting for queue to empty...")
            while self.async_producer.is_alive() and not self.queue.empty() and max_retries > 0:
                max_retries -= 1
                log.debug("Waiting for queue to empty, sleeping one second.")
                time.sleep(1)
        log.debug("Queue is empty, terminating process...")
        self.alive = False
        self.async_producer.kill()
        self.async_producer.join()

    def flush_records(self):
        if self.records:
            log.info("Flushing %d records", len(self.records))
            self.client.put_records(
                StreamName=self.stream_name,
                Records=self.records
            )
            log.debug("Flushed %d records", len(self.records))
            log.info("%d next records", len(self.next_records))
        self.records = self.next_records
        self.next_records = []
