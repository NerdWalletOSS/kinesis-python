try:
    import Queue
except ImportError:
    # Python 3
    import queue as Queue
import logging
import multiprocessing
import time

import boto3

from botocore.exceptions import ClientError

from offspring.process import SubprocessLoop

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class ShardReader(SubprocessLoop):
    """Read from a specific shard, passing records and errors back through queues"""
    # how long we sleep between calls to get_records
    # this follow these best practices: http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    # this can be influeced per-reader instance via the sleep_time arg
    DEFAULT_SLEEP_TIME = 1.0

    def __init__(self, shard_id, shard_iter, record_queue, error_queue, boto3_session=None, sleep_time=None):
        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.boto3_session = boto3_session or boto3.Session()
        self.sleep_time = sleep_time or self.DEFAULT_SLEEP_TIME
        self.start()

    def begin(self):
        """Begin the shard reader main loop"""
        log.info("Shard reader for %s starting", self.shard_id)
        self.client = self.boto3_session.client('kinesis')
        self.retries = 0

    def loop(self):
        """Each loop iteration - returns a sleep time or False to stop the loop"""
        # by default we will sleep for our sleep_time each loop
        loop_status = self.sleep_time

        try:
            resp = self.client.get_records(ShardIterator=self.shard_iter)
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
                # until we complete a successful get_records call
                loop_status = min((
                    30,
                    (self.retries or 1) * 2
                ))
                log.debug("Retrying get_records (#%d %ds): %s", self.retries+1, loop_status, exc)
            else:
                log.error("Client error occurred while reading: %s", exc)
                loop_status = False
        else:
            if not resp['NextShardIterator']:
                # the shard has been closed
                log.info("Our shard has been closed, exiting")
                return False

            self.shard_iter = resp['NextShardIterator']
            self.record_queue.put((self.shard_id, resp))
            self.retries = 0

        return loop_status

    def end(self):
        """End of the main loop"""
        log.info("Shard reader for %s stoping", self.shard_id)


class KinesisConsumer(object):
    """Consume from a kinesis stream

    A process is started for each shard we are to consume from.  Each process passes messages back up to the parent,
    which are returned via the main iterator.
    """
    LOCK_DURATION = 30

    def __init__(self, stream_name, boto3_session=None, state=None, reader_sleep_time=None):
        self.stream_name = stream_name
        self.error_queue = multiprocessing.Queue()
        self.record_queue = multiprocessing.Queue()

        self.boto3_session = boto3_session or boto3.Session()
        self.kinesis_client = self.boto3_session.client('kinesis')

        self.state = state

        self.reader_sleep_time = reader_sleep_time

        self.shards = {}
        self.stream_data = None
        self.run = True

    def state_shard_id(self, shard_id):
        return '_'.join([self.stream_name, shard_id])

    def shutdown_shard_reader(self, shard_id):
        try:
            self.shards[shard_id].shutdown()
            del self.shards[shard_id]
        except KeyError:
            pass

    def setup_shards(self):
        log.debug("Describing stream")
        self.stream_data = self.kinesis_client.describe_stream(StreamName=self.stream_name)
        # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

        setup_again = False
        for shard_data in self.stream_data['StreamDescription']['Shards']:
            # see if we can get a lock on this shard id
            try:
                shard_locked = self.state.lock_shard(self.state_shard_id(shard_data['ShardId']), self.LOCK_DURATION)
            except AttributeError:
                # no self.state
                pass
            else:
                if not shard_locked:
                    # if we currently have a shard reader running we stop it
                    if shard_data['ShardId'] in self.shards:
                        log.warn("We lost our lock on shard %s, stopping shard reader", shard_data['ShardId'])
                        self.shutdown_shard_reader(shard_data['ShardId'])

                    # since we failed to lock the shard we just continue to the next one
                    continue

            # we should try to start a shard reader if the shard id specified isn't in our shards
            if shard_data['ShardId'] not in self.shards:
                log.info("Shard reader for %s does not exist, creating...", shard_data['ShardId'])
                try:
                    iterator_args = self.state.get_iterator_args(self.state_shard_id(shard_data['ShardId']))
                except AttributeError:
                    # no self.state
                    iterator_args = dict(ShardIteratorType='LATEST')

                log.info("%s iterator arguments: %s", shard_data['ShardId'], iterator_args)

                # get our initial iterator
                shard_iter = self.kinesis_client.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_data['ShardId'],
                    **iterator_args
                )

                self.shards[shard_data['ShardId']] = ShardReader(
                    shard_data['ShardId'],
                    shard_iter['ShardIterator'],
                    self.record_queue,
                    self.error_queue,
                    boto3_session=self.boto3_session,
                    sleep_time=self.reader_sleep_time,
                )
            else:
                log.debug(
                    "Checking shard reader %s process at pid %d",
                    shard_data['ShardId'],
                    self.shards[shard_data['ShardId']].process.pid
                )

                if not self.shards[shard_data['ShardId']].process.is_alive():
                    self.shutdown_shard_reader(shard_data['ShardId'])
                    setup_again = True
                else:
                    log.debug("Shard reader %s alive & well", shard_data['ShardId'])

        if setup_again:
            self.setup_shards()

    def shutdown(self):
        for shard_id in self.shards:
            log.info("Shutting down shard reader for %s", shard_id)
            self.shards[shard_id].shutdown()
        self.stream_data = None
        self.shards = {}
        self.run = False

    def __iter__(self):
        try:
            # use lock duration - 1 here since we want to renew our lock before it expires
            lock_duration_check = self.LOCK_DURATION - 1
            while self.run:
                last_setup_check = time.time()
                self.setup_shards()

                while self.run and (time.time() - last_setup_check) < lock_duration_check:
                    try:
                        shard_id, resp = self.record_queue.get(block=True, timeout=0.25)
                    except Queue.Empty:
                        pass
                    else:
                        state_shard_id = self.state_shard_id(shard_id)
                        for item in resp['Records']:
                            if not self.run:
                                break

                            log.debug(item)
                            yield item

                            try:
                                self.state.checkpoint(state_shard_id, item['SequenceNumber'])
                            except AttributeError:
                                # no self.state
                                pass
                            except Exception:
                                log.exception("Unhandled exception check pointing records from %s at %s",
                                              shard_id, item['SequenceNumber'])
                                self.shutdown_shard_reader(shard_id)
                                break

                    shard_id = None
                    try:
                        while True:
                            shard_id = self.error_queue.get_nowait()
                            log.error("Error received from shard reader %s", shard_id)
                            self.shutdown_shard_reader(shard_id)
                    except Queue.Empty:
                        pass

                    if shard_id is not None:
                        # we encountered an error from a shard reader, break out of the inner loop to setup the shards
                        break
        except (KeyboardInterrupt, SystemExit):
            self.run = False
        finally:
            self.shutdown()
