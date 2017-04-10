import atexit
import logging
import multiprocessing
import Queue
import signal
import sys
import time

import boto3

log = logging.getLogger(__name__)


class AsyncProducer(object):
    MAX_SIZE = 2 ** 20

    def __init__(self, stream_name, buffer_time, queue):
        self.stream_name = stream_name
        self.buffer_time = buffer_time
        self.queue = queue
        self.records = []
        self.next_records = []
        self.alive = True
        self.client = boto3.client('kinesis')

        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.alive = False
        self.flush_records()

    def run(self):
        try:
            while self.alive or not self.queue.empty():
                records_size = 0
                timer_start = time.time()

                while (time.time() - timer_start < self.buffer_time):
                    try:
                        data = self.queue.get(block=True, timeout=0.01)
                    except Queue.Empty:
                        continue

                    record = {
                        'Data': data,
                        'PartitionKey': '{0}{1}'.format(time.clock(), time.time()),
                    }

                    records_size += sys.getsizeof(record)
                    if records_size >= self.MAX_SIZE:
                        self.next_records = [record]
                        break

                    self.records.append(record)

                self.flush_records()
        except (SystemExit, KeyboardInterrupt):
            pass
        finally:
            self.flush_records()

    def flush_records(self):
        if self.records:
            self.client.put_records(
                StreamName=self.stream_name,
                Records=self.records
            )

        self.records = self.next_records
        self.next_records = []


class KinesisProducer(object):
    """Produce to Kinesis streams
    """
    def __init__(self, stream_name, buffer_time=0.5):
        self.queue = multiprocessing.Queue()

        async_producer = AsyncProducer(stream_name, buffer_time, self.queue)
        self.process = multiprocessing.Process(target=async_producer.run)

        atexit.register(self.shutdown)
        self.process.start()

    def shutdown(self):
        self.process.terminate()
        self.process.join()

    def put(self, data):
        self.queue.put(data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s:%(lineno)d %(message)s')
    logging.getLogger('botocore').level = logging.INFO
    logging.getLogger('botocore.vendored.requests.packages.urllib3').level = logging.WARN
    producer = KinesisProducer('borgstrom-test')
    for _ in xrange(100):
        producer.put(str(time.time()))
