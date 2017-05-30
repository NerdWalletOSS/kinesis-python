import logging

from kinesis.consumer import KinesisConsumer
from kinesis.state import DynamoDB

log = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(process)d %(name)s:%(lineno)d %(message)s')
logging.getLogger('botocore').level = logging.INFO
logging.getLogger('botocore.vendored.requests.packages.urllib3').level = logging.WARN

consumer = KinesisConsumer(stream_name='borgstrom-test', state=DynamoDB('kinesis-locks'))
for message in consumer:
    log.info("Received message: %s", message)
