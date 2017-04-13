import logging

from kinesis.consumer import KinesisConsumer, FileCheckpoint

log = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(process)d %(name)s:%(lineno)d %(message)s')
logging.getLogger('botocore').level = logging.INFO
logging.getLogger('botocore.vendored.requests.packages.urllib3').level = logging.WARN

checkpointer = FileCheckpoint('/tmp/borgstrom-test.checkpoint')
consumer = KinesisConsumer('borgstrom-test', checkpointer)
for message in consumer:
    log.info("Received message: %s", message)
