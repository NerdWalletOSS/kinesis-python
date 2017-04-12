import atexit
import Queue
import logging
import multiprocessing
import time
import sys

import boto3

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class FileCheckpoint(object):
    """Checkpoint kinesis shard activity to a local file on disk"""

    def __init__(self, filename_base):
        self.filename_base = filename_base
        self.lock = multiprocessing.Lock()

    def shard_filename(self, shard):
        return '.'.join([self.filename_base, shard])

    def get(self, shard):
        try:
            with open(self.shard_filename(shard), 'r') as checkpoint_fd:
                return checkpoint_fd.read()
        except (IOError, OSError):
            pass

    def set(self, shard, position):
        with open(self.shard_filename(shard), 'w') as checkpoint_fd:
            checkpoint_fd.write(position)


class ShardReader(object):
    """Read from a specific shard, passing records and errors back through queues"""
    def __init__(self, shard_id, shard_iter, record_queue, error_queue):
        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.process = multiprocessing.Process(target=self.run)
        self.process.start()

    def run(self):
        log.info("Shard reader for %s starting", self.shard_id)
        client = boto3.client('kinesis')
        try:
            while True:
                resp = client.get_records(ShardIterator=self.shard_iter)

                if not resp['NextShardIterator']:
                    # the shard has been closed
                    break

                self.shard_iter = resp['NextShardIterator']

                if len(resp['Records']) == 0:
                    time.sleep(0.1)
                else:
                    for record in resp['Records']:
                        self.record_queue.put(record)
        except (SystemExit, KeyboardInterrupt):
            pass
        except ClientError as exc:
            log.error("Client error occurred while reading: %s", exc)
        except Exception:
            log.exception("Unhandled exception in shard reader %s", self.shard_id)
        finally:
            self.error_queue.put(self.shard_id)
            sys.exit()

    def stop(self):
        log.info("Shard reader for %s stoping", self.shard_id)
        self.process.terminate()
        self.process.join()


class KinesisConsumer(object):
    """Consume from a kinesis stream

    A process is started for each shard we are to consume from.  Each process passes messages back up to the parent,
    which are returned via the main iterator.

    TODO:
    * checkpoint -- we should checkpoint to backends (start with file, expand to dynamo) if no checkpoint is provided
    then we use LATEST as our type, otherwise we resume at the record in our checkpoint data.
    """

    def __init__(self, stream_name, checkpointer=None, checkpoint_interval=5):
        self.stream_name = stream_name
        self.checkpointer = checkpointer
        self.checkpoint_interval = checkpoint_interval
        self.error_queue = multiprocessing.Queue()
        self.record_queue = multiprocessing.Queue()
        self.client = boto3.client('kinesis')
        self.shards = {}
        self.stream_data = None
        self.run = True

        # we shutdown our shard readers at the end of our iterator loop
        # but to ensure that we don't orphan any child processes we explicitly shutdown at exit
        atexit.register(self.shutdown)

    def setup_shards(self):
        log.debug("Setting up shards")
        if self.stream_data is None:
            log.debug("Describing stream")
            self.stream_data = self.client.describe_stream(StreamName=self.stream_name)
            # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

        setup_again = False
        for shard_data in self.stream_data['StreamDescription']['Shards']:
            if shard_data['ShardId'] not in self.shards:
                log.debug("Shard reader for %s does not exist, creating...", shard_data['ShardId'])
                # XXX TODO: load from checkpoint
                # get our initial iterator
                shard_iter = self.client.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_data['ShardId'],
                    ShardIteratorType='LATEST'
                )

                self.shards[shard_data['ShardId']] = ShardReader(
                    shard_data['ShardId'],
                    shard_iter['ShardIterator'],
                    self.record_queue,
                    self.error_queue
                )
            else:
                log.debug("Checking shard reader %s process", shard_data['ShardId'])
                if not self.shards[shard_data['ShardId']].process.is_alive():
                    self.shards[shard_data['ShardId']].stop()
                    del self.shards[shard_data['ShardId']]

                    # invalidate stream_data since our shard is no longer alive
                    self.stream_data = None
                    setup_again = True

        # if any of our shards were dead and we invalidated the stream data we need to run the shard setup again
        if setup_again:
            self.setup_shards()

    def shutdown(self):
        for shard_id in self.shards:
            self.shards[shard_id].stop()
        self.stream_data = None
        self.shards = {}
        self.run = False

    def __iter__(self):
        try:
            while self.run:
                self.setup_shards()

                while True:
                    try:
                        item = self.record_queue.get(block=True, timeout=0.25)
                        yield item
                    except Queue.Empty:
                        pass

                    try:
                        shard_reader_error = self.error_queue.get_nowait()
                    except Queue.Empty:
                        pass
                    else:
                        log.error("Error received from shard reader %s", shard_reader_error)
                        break
        finally:
            self.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s:%(lineno)d %(message)s')
    logging.getLogger('botocore').level = logging.INFO
    logging.getLogger('botocore.vendored.requests.packages.urllib3').level = logging.WARN
    try:
        checkpointer = FileCheckpoint('/tmp/borgstrom-test.checkpoint')
        consumer = KinesisConsumer('borgstrom-test', checkpointer)
        for message in consumer:
            log.info("Received message: %s", message)
    except KeyboardInterrupt:
        pass
