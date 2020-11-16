from kinesis.consumer import KinesisConsumer

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def test_setup_shards(mocker):
    mock_boto3_session = MagicMock()
    mock_shard_reader = mocker.patch('kinesis.consumer.ShardReader')

    consumer = KinesisConsumer('testing', boto3_session=mock_boto3_session)

    mock_boto3_session.client.assert_called_with('kinesis', endpoint_url=None)

    consumer.kinesis_client.describe_stream.return_value = {
        'StreamDescription': {
            'Shards': [
                {
                    'ShardId': 'test-shard',
                }
            ]
        }
    }
    consumer.kinesis_client.get_shard_iterator.return_value = {
        'ShardIterator': 'test-iter'
    }

    consumer.setup_shards()

    consumer.kinesis_client.describe_stream.assert_called_with(StreamName='testing')
    consumer.kinesis_client.get_shard_iterator.assert_called_with(
        StreamName='testing',
        ShardId='test-shard',
        ShardIteratorType='LATEST'
    )

    mock_shard_reader.assert_called_with(
        'test-shard',
        'test-iter',
        consumer.record_queue,
        consumer.error_queue,
        boto3_session=consumer.boto3_session,
        endpoint_url=None,
        sleep_time=consumer.reader_sleep_time
    )
