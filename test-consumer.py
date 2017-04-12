import logging

from kinesis.consumer import KinesisConsumer, FileCheckpoint


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
