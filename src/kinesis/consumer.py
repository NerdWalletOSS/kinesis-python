import Queue
import logging
import multiprocessing
import time
import random
import socket
import signal
import sys

import boto3

from botocore.exceptions import ClientError

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class ShardReader(object):
    """Read from a specific shard, passing records and errors back through queues"""
    # how long we sleep between calls to get_records
    DEFAULT_SLEEP_TIME = 0.25

    def __init__(self, shard_id, shard_iter, record_queue, error_queue, boto3_session=None, sleep_time=None):
        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.boto3_session = boto3_session or boto3.Session()
        self.sleep_time = sleep_time or self.DEFAULT_SLEEP_TIME

        # the alive attribute is used to control the main reader loop
        # once our run process has started changing this flag in the parent process has no effect
        # it is changed by our signal handler within the child process
        self.alive = True

        self.process = multiprocessing.Process(target=self.run)
        self.process.start()

    def run(self):
        """Run the shard reader main loop

        This is called as the target of a multiprocessing Process.
        """
        log.info("Shard reader for %s starting", self.shard_id)

        signal.signal(signal.SIGTERM, self.signal_handler)

        client = self.boto3_session.client('kinesis')

        retries = 0
        try:
            while self.alive:
                sleep_time = self.sleep_time

                try:
                    resp = client.get_records(ShardIterator=self.shard_iter)
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                        log.error("Client error occurred while reading: %s", exc)
                        break

                    # sleep for 0.5 second the first loop, 1 second the next, 2, 4, 6, 8, ..., up to a max of 30
                    sleep_time = min((
                        30,
                        (retries or self.sleep_time) * 2
                    ))
                    log.debug("Retrying get_records (#%d %ds): %s", retries+1, sleep_time, exc)
                else:
                    if not resp['NextShardIterator']:
                        # the shard has been closed
                        break

                    self.shard_iter = resp['NextShardIterator']
                    self.record_queue.put((self.shard_id, resp))
                    retries = 0

                time.sleep(sleep_time)

        except (SystemExit, KeyboardInterrupt):
            log.debug("Exit via interrupt")
        except Exception:
            log.exception("Unhandled exception in shard reader %s", self.shard_id)
        finally:
            self.error_queue.put_nowait(self.shard_id)
            sys.exit()

    def signal_handler(self, signum, frame):
        """Handle signals within our child process to terminate the main loop"""
        log.info("Caught signal %s", signum)
        self.alive = False

    def stop(self):
        """Stop the child process started for this shard reader

        This should only be called by the parent process, never from within the main run loop.
        """
        if self.process:
            log.info("Shard reader for %s stoping", self.shard_id)
            self.process.terminate()
            self.process.join()
            self.process = None


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

    def setup_shards(self):
        log.debug("Describing stream")
        self.stream_data = self.kinesis_client.describe_stream(StreamName=self.stream_name)
        # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

        setup_again = False
        for shard_data in self.stream_data['StreamDescription']['Shards']:
            # see if we can get a lock on this shard id
            try:
                shard_locked = self.state.lock_shard(self.state_shard_id(shard_data['ShardId']), self.LOCK_DURATION)
            except TypeError:
                # no self.state
                pass
            else:
                if not shard_locked:
                    # if we currently have a shard reader running we stop it
                    if shard_data['ShardId'] in self.shards:
                        log.warn("We lost our lock on shard %s, stopping shard reader", shard_data['ShardId'])
                        self.shards[shard_data['ShardId']].stop()
                        del self.shards[shard_data['ShardId']]

                    # since we failed to lock the shard we just continue to the next one
                    continue

            # we should try to start a shard reader if the shard id specified isn't in our shards
            if shard_data['ShardId'] not in self.shards:
                log.debug("Shard reader for %s does not exist, creating...", shard_data['ShardId'])
                try:
                    iterator_args = self.state.get_iterator_args(self.state_shard_id(shard_data['ShardId']))
                except TypeError:
                    # no self.state
                    iterator_args = dict(ShardIteratorType='LATEST')

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
                    self.shards[shard_data['ShardId']].stop()
                    del self.shards[shard_data['ShardId']]

                    setup_again = True
                else:
                    log.debug("Shard reader %s alive & well", shard_data['ShardId'])

        # if any of our shards were dead and we invalidated the stream data we need to run the shard setup again
        if setup_again:
            self.setup_shards()

    def shutdown(self):
        for shard_id in self.shards:
            log.info("Shutting shard reader for %s", shard_id)
            self.shards[shard_id].stop()
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
                    except (KeyboardInterrupt, SystemExit):
                        self.run = False
                        break
                    else:
                        for item in resp['Records']:
                            if not self.run:
                                break

                            yield item

                            try:
                                self.state.checkpoint(self.state_shard_id(shard_id), item['SequenceNumber'])
                            except TypeError:
                                # no self.state
                                pass

                    # this avoids logging any errors if we're shutting down
                    if not self.run:
                        break

                    try:
                        shard_reader_error = self.error_queue.get_nowait()
                    except Queue.Empty:
                        pass
                    else:
                        log.error("Error received from shard reader %s", shard_reader_error)
                        break
        finally:
            self.shutdown()
