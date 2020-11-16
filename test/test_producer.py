import sys

from kinesis.producer import KinesisProducer, sizeof


def test_producer(mocker):
    mocked_async_producer = mocker.patch('kinesis.producer.AsyncProducer')
    producer = KinesisProducer('testing')
    mocked_async_producer.assert_called_with(
        'testing',
        0.5,
        producer.queue,
        max_count=None,
        max_size=None,
        boto3_session=None,
        endpoint_url=None
    )

    mocked_queue = mocker.patch.object(producer, 'queue')
    producer.put('foo', explicit_hash_key='hash', partition_key='partition')
    mocked_queue.put.assert_called_with(('foo', 'hash', 'partition'))


def test_sizeof():
    s1 = "a"
    assert sizeof(s1) == sys.getsizeof(s1)

    s2 = ["a", "b", "a"]
    assert sizeof(s2) == sys.getsizeof(s2) + sys.getsizeof("a") + sys.getsizeof("b")

    s3 = {"a": "a", "b": "a", "c": 1}
    assert sizeof(s3) == sys.getsizeof(s3) + sys.getsizeof("a") + sys.getsizeof("b") + sys.getsizeof("c") + \
        sys.getsizeof(1)
