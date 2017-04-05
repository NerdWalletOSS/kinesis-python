import atexit
import sys
import time

import boto3


class KinesisProducer(object):
    """Produce to Kinesis streams
    """
    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.client = boto3.client('kinesis')
        self.partition_key = 0

    def put(self, data):
        self.client.put_record(
            StreamName=self.stream_name,
            Data=data,
            PartitionKey='kp{0}'.format(self.partition_key)
        )

        try:
            self.partition_key += 1
        except ValueError:
            self.partition_key = 0



if __name__ == '__main__':
    producer = KinesisProducer('borgstrom-test')
    for _ in xrange(100):
        producer.put(str(time.time()))
