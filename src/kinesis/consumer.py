import logging
from multiprocessing import Process, Queue, Lock, Manager
try:
    # Python 3
    import queue
except ImportError:
    import Queue as queue
import time
import random
import boto3
from botocore.exceptions import ClientError
from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class KinesisConsumer(object):
    # Set this to the number of seconds to sleep so that other workers can pick up shards after our lock runs out.
    # This helps a lot when you have a docker swarm of consumers on the same stream so that one container does not
    # bottleneck the whole consumption.  Set to zero to ignore.
    GREEDY_GRACE_TIME = random.random()

    def __init__(self, stream_name, state=None, max_queue_size=None, lock_duration=30,
                 boto3_session=None, sleep_time=1, be_greedy=True):
        log.debug("Creating Consumers")
        self.stream_name = stream_name
        self.lock_duration = lock_duration
        self.boto3_session = boto3_session or boto3.Session()
        self.max_queue_size = max_queue_size
        self.sleep_time = sleep_time
        self.kinesis_client = self.boto3_session.client('kinesis')
        self.state = state
        self.shards = {}
        self.shards_lock = Lock()
        self.stream_data = None
        self.be_greedy = be_greedy
        self.run = True
        self.manager = Manager()
        if self.max_queue_size:
            self.record_queue = self.manager.Queue(maxsize=self.max_queue_size)
        else:
            self.record_queue = self.manager.Queue()
        self.restart_queue = self.manager.Queue()
        # Get things started...
        self.client = self.boto3_session.client('kinesis')
        self.manager_proc = None

    def __iter__(self):
        self.start()
        return self

    def __next__(self):
        return self.get_record()

    def start(self):
        log.debug("Starting Consumers")
        if hasattr(self, "manager_proc") and (self.manager_proc is None or
                self.manager_proc.is_alive() is False):
            log.info("Starting shard manager...")
            self.start_shard_manager()
            log.info("Shard manager started.")

    def get_record(self):
        try_again = True
        item = None
        while try_again:
            try_again = False
            try:
                # shard_id, resp = self.record_queue.get(block=True, timeout=queue_timeout)
                log.debug("Getting record from consumer record queue size {0} object {1}...".format(
                    self.record_queue.qsize(), self.record_queue))
                if not self.record_queue.empty():
                    log.debug("Queue is not empty, getting record from consumer record queue...")
                    shard_id, item = self.record_queue.get()
                else:
                    raise queue.Empty
                log.debug("Got record from consumer record queue.")
            except queue.Empty:
                log.debug("Consumer queue is empty.")
                time.sleep(0.25)
                try_again = True
                pass
            except OSError as exc:
                log.exception("OSError Exception: {0}".format(exc))
                # This is a memory problem, most likely.
                log.error("Shutting down...")
                self.run = False
                self.shutdown()
                return None
            except Exception as exc:
                log.exception("UNHANDLED EXCEPTION {0}".format(exc))
                log.error("Shutting down...")
                self.run = False
                self.shutdown()
                return None
            else:
                log.debug("Processing item from consumer record queue.")
                state_shard_id = self.state_shard_id(shard_id)
                log.debug("Got item from from queue for shard {0}".format(shard_id))
                try:
                    log.debug("Attempting to checkpoint item for shard {0}...".format(shard_id))
                    checkpoint_status = self.state.checkpoint(state_shard_id, item['SequenceNumber'])
                except AttributeError:
                    # no self.state
                    log.debug("No state table...")
                    pass
                except Exception as exc:
                    log.exception("Unhandled exception check pointing records from {0} at {1} Exception: {2}".format(
                        shard_id, item['SequenceNumber'], exc))
                    log.warning("Restarting shards...")
                    if self.restart_queue.empty():
                        self.restart_queue.put("restart", block=False)
                    try_again = True
                else:
                    if checkpoint_status:
                        log.debug("Checkpointed item...")
                        try_again = False
                    else:
                        log.debug("Could not checkpoint item, ignoring for shard {0}...".format(shard_id))
                        log.debug("Restarting shards...")
                        if self.restart_queue.empty():
                            self.restart_queue.put("restart", block=False)
                        try_again = True
        log.debug("Returning item...")
        return item

    def start_shard_manager(self):
        log.debug("Shard manager starting...")
        self.manager_proc = Process(target=self._shard_manager)
        self.manager_proc.start()
        log.debug("Shard manager started.")

    def _shard_manager(self):
        try:
            # use lock duration - 1 here since we want to renew our lock before it expires
            lock_duration_check = self.lock_duration - 1
            while self.run:
                log.debug("Checking shards")
                last_setup_check = time.time()
                self.setup_shards()

                time.sleep(lock_duration_check)

                while self.run and (time.time() - last_setup_check) < lock_duration_check:
                    log.debug("Manager")
                    msg = None
                    try:
                        while True:
                            msg = self.restart_queue.get(block=False)
                            log.debug("Got restart message: {0}".format(msg))
                    except queue.Empty:
                        if msg:
                            log.debug("Got message to restart shards.")
                            self.setup_shards()
                    time.sleep(0.1)
        except (KeyboardInterrupt, SystemExit):
            self.run = False

    def setup_shards(self):
        with self.shards_lock:
            log.debug("Setup Shards")
            self.stream_data = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

            setup_again = False
            # Let's add some randomness into shard locking so one worker does not take over completely...
            stream_shard_data = list(self.stream_data['StreamDescription']['Shards'])
            # log.debug("Got shards: {0}".format(stream_shard_data))
            if stream_shard_data:
                random.shuffle(stream_shard_data)
            for shard_data in stream_shard_data:
                log.debug("Attempting to lock shard: {0}".format(shard_data['ShardId']))
                # see if we can get a lock on this shard id
                try:
                    shard_locked = self.state.lock_shard(self.state_shard_id(shard_data['ShardId']), self.lock_duration)
                    log.debug("Shard {0} lock status: {1}".format(shard_data['ShardId'], shard_locked))
                except AttributeError:
                    # no self.state
                    pass
                else:
                    if not shard_locked:
                        # if we currently have a shard reader running we stop it
                        if shard_data['ShardId'] in self.shards:
                            log.info("We lost our lock on shard %s, stopping shard reader", shard_data['ShardId'])
                            self._shutdown_shard_reader(shard_data['ShardId'])

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

                    log.debug("%s iterator arguments: %s", shard_data['ShardId'], iterator_args)

                    # get our initial iterator
                    shard_iter = self.kinesis_client.get_shard_iterator(
                        StreamName=self.stream_name,
                        ShardId=shard_data['ShardId'],
                        **iterator_args
                    )

                    self.shards[shard_data['ShardId']] = Process(target=self._run_reader,
                                                                 args=(shard_data['ShardId'],
                                                                       shard_iter['ShardIterator'],
                                                                       self.record_queue))
                    self.shards[shard_data['ShardId']].start()
                else:
                    log.debug(
                        "Checking shard reader %s process at pid %d",
                        shard_data['ShardId'],
                        self.shards[shard_data['ShardId']].pid
                    )

                    if not self.shards[shard_data['ShardId']].is_alive():
                        self._shutdown_shard_reader(shard_data['ShardId'])
                        setup_again = True
                    else:
                        log.debug("Shard reader %s alive & well", shard_data['ShardId'])
                # Don't be greedy, allow other waiting workers a shot at the extra shards.
                if len(self.shards) > 1 and not self.be_greedy:
                    log.debug("Sleeping so we are not greedy!")
                    time.sleep(self.GREEDY_GRACE_TIME)
        if setup_again:
            self.setup_shards()

    def state_shard_id(self, shard_id):
        return '_'.join([self.stream_name, shard_id])

    def _shutdown_shard_reader(self, shard_id):
        #
        #  We must be holding the self.shards_lock to get into this function!
        #
        log.info("Shutting down shard reader {0}".format(shard_id))
        try:
            log.debug("Killing process for shard reader {0}".format(shard_id))
            if self.shards[shard_id].is_alive():
                self.shards[shard_id].kill()
                log.debug("Joining killed process for shard {0}...".format(shard_id))
                self.shards[shard_id].join()
                log.debug("Finished process join for shard {0}".format(shard_id))
            else:
                log.debug("Process {0} for shard reader {1} is dead.".format(self.shards[shard_id].pid, shard_id))
            log.debug("Deleting the reader from active shards...")
            del self.shards[shard_id]
            log.info("Completed shutdown of shard reader {0}".format(shard_id))
        except KeyError:
            log.debug("Got KeyError while trying to shutdown shard reader {0}".format(shard_id))
            pass

    def shutdown(self):
        log.info("Shutting down all shard readers...")
        with self.shards_lock:
            to_shut_down = self.shards
            for proc in to_shut_down:
                log.info("Shutting down a shard reader {0}...".format(proc))
                self.shards[proc].kill()
                self.shards[proc].join()
                # log.info("Shutting down shard reader for %s", shard_id)
                # self.shutdown_shard_reader(shard_id)
        self.stream_data = None
        self.shards = {}
        self.run = False
        log.info("All shard readers have been shutdown.")
        log.info("Shutting down shard manager...")
        if hasattr(self, "manager_proc"):
            self.manager_proc.kill()
            self.manager_proc.join()
        log.info("Shard manager has been shutdown.")

    def _run_reader(self, shard_id, shard_iter, record_queue):
        curr_iter = shard_iter
        retries = 0
        while self.run:
            try:
                resp = self.client.get_records(ShardIterator=curr_iter)
            except ClientError as exc:
                if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                    # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
                    # until we complete a successful get_records call
                    sleep_time = min((
                        30,
                        (retries or 1) * 2
                    ))
                    log.debug("Retrying get_records for shard %s (#%d %ds): %s", shard_id, retries+1, sleep_time, exc)
                else:
                    log.exception("Client error occurred while reading: %s", exc)
                    return False
            else:
                if not resp['NextShardIterator']:
                    # the shard has been closed
                    log.debug("Our shard {0} has been closed, exiting".format(shard_id))
                    return False
                curr_iter = resp['NextShardIterator']
                log.debug("Got {0:,} records from kinesis stream on shard {1}.".format(len(resp['Records']), shard_id))
                queue_add_count = 0
                for item in resp['Records']:
                    try:
                        record_queue.put((shard_id, item))
                        # log.debug("Put record on record queue for shard {0} {1} {2}.".format(shard_id,
                        #                                                                      record_queue.qsize(),
                        #                                                                      record_queue))
                        queue_add_count += 1
                    except OSError as exc:
                        log.exception("OSError Exception: {0}".format(exc))
                        # This is a memory problem, most likely.
                        log.error("Shutting down...")
                        return False
                    except Exception as exc:
                        log.exception("UNHANDLED EXCEPTION {0}".format(exc))
                        log.error("Shutting down...")
                        return False
                log.debug("Put {0:,} records on the consumer queue.".format(queue_add_count))
                retries = 0

            if self.sleep_time:
                log.debug("Reader sleeping for shard {0}...".format(shard_id))
                time.sleep(self.sleep_time)
