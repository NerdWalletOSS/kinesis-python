import logging
import socket
import time

import boto3

from botocore.exceptions import ClientError

from .exceptions import RETRY_EXCEPTIONS

log = logging.getLogger(__name__)


class DynamoDB(object):
    def __init__(self, table_name, boto3_session=None):
        self.boto3_session = boto3_session or boto3.Session()

        self.dynamo_resource = self.boto3_session.resource('dynamodb')
        self.dynamo_table = self.dynamo_resource.Table(table_name)

        self.shards = {}

    def get_iterator_args(self, shard_id):
        try:
            return dict(
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=self.shards[shard_id]['seq']
            )
        except KeyError:
            return dict(
                ShardIteratorType='LATEST'
            )

    def checkpoint(self, shard_id, seq):
        fqdn = socket.getfqdn()

        try:
            # update the seq attr in our item
            # ensure our fqdn still holds the lock and the new seq is bigger than what's already there
            self.dynamo_table.update_item(
                Key={'shard': shard_id},
                UpdateExpression="set seq = :seq",
                ConditionExpression="fqdn = :fqdn AND (attribute_not_exists(seq) OR seq < :seq)",
                ExpressionAttributeValues={
                    ':fqdn': fqdn,
                    ':seq': seq,
                }
            )
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to read lock table in Dynamo: %s", exc)
                time.sleep(1)

            # for all other exceptions (including condition check failures) we just re-raise
            raise

    def lock_shard(self, shard_id, expires):
        dynamo_key = {'shard': shard_id}
        fqdn = socket.getfqdn()
        now = time.time()
        expires = int(now + expires)  # dynamo doesn't support floats

        try:
            # Do a consistent read to get the current document for our shard id
            resp = self.dynamo_table.get_item(Key=dynamo_key, ConsistentRead=True)
            self.shards[shard_id] = resp['Item']
        except KeyError:
            # if there's no Item in the resp then the document didn't exist
            pass
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to read lock table in Dynamo: %s", exc)
                time.sleep(1)
                return self.lock_shard(shard_id)

            # all other client errors just get re-raised
            raise
        else:
            if fqdn != self.shards[shard_id]['fqdn'] and now < self.shards[shard_id]['expires']:
                # we don't hold the lock and it hasn't expired
                log.debug("Not starting reader for shard %s -- locked by %s until %s",
                          shard_id, self.shards[shard_id]['fqdn'], self.shards[shard_id]['expires'])
                return False

        try:
            # Try to acquire the lock by setting our fqdn and calculated expires.
            # We add a condition that ensures the fqdn & expires from the document we loaded hasn't changed to
            # ensure that someone else hasn't grabbed a lock first.
            self.dynamo_table.update_item(
                Key=dynamo_key,
                UpdateExpression="set fqdn = :new_fqdn, expires = :new_expires",
                ConditionExpression="fqdn = :current_fqdn AND expires = :current_expires",
                ExpressionAttributeValues={
                    ':new_fqdn': fqdn,
                    ':new_expires': expires,
                    ':current_fqdn': self.shards[shard_id]['fqdn'],
                    ':current_expires': self.shards[shard_id]['expires'],
                }
            )
        except KeyError:
            # No previous lock - this occurs because we try to reference the shard info in the attr values but we don't
            # have one.  Here our condition prevents a race condition with two readers starting up and both adding a
            # lock at the same time.
            resp = self.dynamo_table.update_item(
                Key=dynamo_key,
                UpdateExpression="set fqdn = :new_fqdn, expires = :new_expires",
                ConditionExpression="attribute_not_exists(#shard_id)",
                ExpressionAttributeValues={
                    ':new_fqdn': fqdn,
                    ':new_expires': expires,
                },
                ExpressionAttributeNames={
                    # 'shard' is a reserved word in expressions so we need to use a bound name to work around it
                    '#shard_id': 'shard',
                },
                ReturnValues='ALL_NEW'
            )
        except ClientError as exc:
            if exc.response['Error']['Code'] == "ConditionalCheckFailedException":
                # someone else grabbed the lock first
                return False

            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                log.warn("Throttled while trying to write lock table in Dynamo: %s", exc)
                time.sleep(1)
                return self.should_start_shard_reader(shard_id)

        # we now hold the lock (or we don't use dynamo and don't care about the lock)
        return True
