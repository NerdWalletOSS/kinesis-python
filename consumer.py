import atexit
import Queue
import logging
import multiprocessing
import time

import boto3

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

        log.info("Shard reader for %s starting", self.shard_id)
        self.alive = True
        self.process.start()

    def run(self):
        client = boto3.client('kinesis')
        while self.alive:
            resp = client.get_records(ShardIterator=self.shard_iter)

            if not resp['NextShardIterator']:
                # the shard has been closed
                self.alive = False
                break
            self.shard_iter = resp['NextShardIterator']

            if len(resp['Records']) == 0:
                time.sleep(0.25)
            else:
                for record in resp['Records']:
                    self.record_queue.put(record)

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.join()
            self.alive = False
            self.process = None


class KinesisConsumer(object):
    """Consume from a kinesis stream

    A process is started for each shard we are to consume from.  Each process passes messages back up to the parent,
    which are returned via the main iterator.

    TODO:
    * checkpoint -- we should checkpoint to backends (start with file, expand to dynamo) if no checkpoint is provided
    then we use LATEST as our type, otherwise we resume at the record in our checkpoint data.
    """

    def __init__(self, stream_name, checkpointer, checkpoint_interval=5):
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
        # but to ensure that we don't orphan any child processes 
        atexit.register(self.shutdown_shards)

    def setup_shards(self):
        if self.stream_data is None:
            log.debug("Describing stream")
            self.stream_data = self.client.describe_stream(StreamName=self.stream_name)
            # XXX TODO: handle StreamStatus

        for shard_data in self.stream_data['StreamDescription']['Shards']:
            if shard_data['ShardId'] not in self.shards:
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
                if not self.shards[shard_data['ShardId']].alive:
                    del self.shards[shard_data['ShardId']]
                    
                    # invalidate stream_data since our shard is no longer alive
                    self.stream_data = None

    def shutdown_shards(self):
        for shard_id in self.shards:
            self.shards[shard_id].stop()
        self.stream_data = None
        self.shards = {}
        self.run = False

    def __iter__(self):
        while self.run:
            self.setup_shards()

            try:
                # as long as there is data available from our shard processes we read from the queue in a tight loop
                # for up to 1000 items before we drop back out to let our error handling and maintenance tasks run
                tight_count = 1000
                while tight_count > 0:
                    item = self.record_queue.get(block=True, timeout=0.25)
                    yield item
                    tight_count -= 1

            except Queue.Empty:
                pass

            except Exception:
                log.exception("Unhandled exception while reading from shards!")
                self.shutdown_shards()
                raise

        self.shutdown_shards()


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
        consumer.shutdown_shards()
