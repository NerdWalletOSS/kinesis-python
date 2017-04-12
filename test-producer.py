import logging

from kinesis.producer import KinesisProducer


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s:%(lineno)d %(message)s')
logging.getLogger('botocore').level = logging.INFO
logging.getLogger('botocore.vendored.requests.packages.urllib3').level = logging.WARN
producer = KinesisProducer('borgstrom-test')
for idx in xrange(100):
    producer.put(str(idx))
