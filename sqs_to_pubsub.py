#!/usr/bin/env python3
#
# code taken mostly from:
# https://github.com/mozilla-services/tokenserver/blob/810117d/tokenserver/scripts/process_account_events.py
#

import os, sys, time
import json
import logging

import hashlib

import boto3

from google.cloud import pubsub_v1

project_id            = os.environ.get('PUBSUB_PROJECT_ID',          '')
topic_name            = os.environ.get('FXA_PUBSUB_TOPIC',           '')
sqs_queue_url         = os.environ.get('SQS_QUEUE_URL',              '')
aws_access_key_id     = os.environ.get('SQS_QUEUE_ACCESS_KEY_ID',    '')
aws_secret_access_key = os.environ.get('SQS_QUEUE_SECRET_ACCESS_KEY','')

if not project_id or not topic_name or not sqs_queue_url or not aws_access_key_id or not aws_secret_access_key:
    raise Exception('Missing ENV variables')

logger = logging.getLogger("sqs_to_pubsub")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def process_account_events(aws_region='us-west-2',
                           queue_wait_time=20):
    """Process account events from an SQS queue.
    This function polls the specified SQS queue for account-realted events,
    processing each as it is found.  It polls indefinitely and does not return;
    to interrupt execution you'll need to e.g. SIGINT the process.
    """
    logger.info("Processing account events from %s", sqs_queue_url)
    try:
        # Connect to the SQS queue.
        # If no region is given, infer it from the instance metadata.
        logger.debug("Connecting to queue %r in %r", sqs_queue_url, aws_region)
        client = boto3.client('sqs',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
        i = 0
        while True:
          #i += 1
          #if i > 500:
            #break
          response = client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,
            AttributeNames=[])
          #print(response)
          for msg in response['Messages']:
            msg_body       = msg['Body']
            receipt_handle = msg['ReceiptHandle']
            #print(msg_body)
            process_account_event(msg_body)
            #print(f"Deleting message with ReceiptHandle ")
            del_response = client.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=receipt_handle
            )
          #sys.exit()

        print("Sleeping for a while to ensure all pub/sub msgs are sent")
        time.sleep(10)
        sys.exit()

    except Exception:
        logger.exception("Error while processing account events")
        raise

def send_event_to_pubsub(event):
  event_json = json.dumps(event)
  final_msg = { "Message": event_json }
  final_json = json.dumps(final_msg)
  #print(final_json)


  def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

  # Data must be a bytestring
  data = final_json.encode('utf-8')
  # When you publish a message, the client returns a Future.
  message_future = publisher.publish(topic_path, data=data)
  message_future.add_done_callback(callback)

print('Published message IDs:')

def process_account_event(body):
    """Parse and process a single account event."""
    # Try very hard not to error out if there's junk in the queue.
    email = None
    event_type = None
    generation = None
    try:
        body = json.loads(body)
        event = json.loads(body['Message'])
        event_type = event["event"]
        uid = event["uid"].encode('utf-8')
        #uid = uid.encode('utf-8')

        #print("Received event:")
        #print(event)

        if 'email' in event:
          event['email'] = 'fakeemail@mozilla.com'

        event['uid'] = hashlib.sha3_224( uid ).hexdigest()
        
        #print("Transformed event:")
        #print(event)

        send_event_to_pubsub(event)

    except (ValueError, KeyError) as e:
        logger.exception("Invalid account message: %s", e)

if __name__ == "__main__":
  process_account_events()
