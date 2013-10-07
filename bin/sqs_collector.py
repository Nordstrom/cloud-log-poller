import os, logging, datetime
import boto
from datetime import time, datetime
import json
import boto 
import boto.sqs
import inflection
from boto.sqs.message import Message, RawMessage
from boto.sqs.jsonmessage import JSONMessage
from nose.tools import set_trace

logger = logging.getLogger('cloud_log_poller')

class SqsCollector:
  def __init__(self, config, transports):
    self.__transports = transports
    
    config_defaults = {
      'num_messages_to_get': 10, 
      'queue_wait_time': 20, 
      'flush_events_threshold': 50,
      'json_messages': True
    }

    self.__config = dict(config_defaults.items() + config.items())

    # Lookup the region by name
    region = None
    for r in boto.sqs.regions():
      if r.name == config['aws_region']:
        region = r

    sqs_connection = boto.connect_sqs(
      region=region, 
      aws_access_key_id=config['aws_access_key_id'], 
      aws_secret_access_key=config['aws_secret_access_key'])

    self.queue = sqs_connection.get_queue(config['queue_name'])
    self.queue.set_message_class(RawMessage)

    if self.queue is None:
      logger.error("Could not find SQS queue %s" % config['queue_name'])

  def run(self):
    logger.debug("Running SqsCollector")
    if self.queue is None:
      return

    events = []
    logger.debug("Polling sqs queue")

    while True:
      messages = self.queue.get_messages(
        num_messages=self.__config['num_messages_to_get'], 
        wait_time_seconds=self.__config['queue_wait_time'])

      logger.info("Received %s SQS messages" % len(messages))
      for message in messages:
        message_body = message.get_body()
        try:
          event = json.loads(message_body)
        except ValueError:
          logger.debug("Could not parse message_body as json: %s" % message_body)
          event = { 'value': message_body }

        events.append(event)

      if len(messages) > 0:
        for message in messages:
          # Delete the received messages from SQS
          self.queue.delete_message_batch(messages)
      else:
        # Break out of the loop once we get no more messages back
        break
      
    if len(events) > 0:
      logger.info("Sending %s SQS Messages to transports" % len(events))     
      for transport in self.__transports:
        transport.send(self.__config['queue_name'], 'sqs', events)
    else:
      logger.info("Found no SQS messages in queue")


    