import os
import json
import logging
from datetime import datetime
from dateutil import tz
import time
import inflection
import splunklib.client as splunk_client
import util 
from dateutil import parser

logger = logging.getLogger('cloud_log_poller')

class SplunkTransport:
  def __init__(self, config):
    self.config = config

    self.splunk_service = splunk_client.connect(
      host=config['splunk_host'],
      port=config['splunk_port'],
      username=config['splunk_username'],
      password=config['splunk_password'])

    self.splunk_index = self.splunk_service.indexes[config['splunk_index']]
    self.local_zone = tz.tzlocal()
    self.utc_zone = tz.tzutc()

  def send(self, source, sourcetype, log_events):
    logger.info("Sending %s events to Splunk, source=%s, sourcetype=%s" 
      % (len(log_events), source, sourcetype))

    stream =  self.splunk_index.attach()

    for event in log_events:
      util.underscore_keys(event)

      # Normalize timestamps
      if 'timestamp' in event:
        if (isinstance(event['timestamp'], basestring)):
          event['timestamp'] = parser.parse(event['timestamp'])

        if (isinstance(event['timestamp'], datetime)):
          if event['timestamp'].tzinfo == self.utc_zone:
            utc_time = event['timestamp']
          else:
            utc_time = event['timestamp'].replace(tzinfo=self.utc_zone)

          # local_time = utc_time.astimezone(self.local_zone)
          event['timestamp'] = utc_time.isoformat()

      if len(event) == 0:
        logger.debug("Skipping event due to no values")

      if len(event) > 0:
        try:
          json_event = json.dumps(event, cls=SplunkEncoder)
          logger.debug("Sending event: %s" % json_event)
        except:
          logger.info("Could not JSON serialize log event: %s" % repr(event))
          continue

        # The splunk socket keeps concactenating multiple events into a single splunk entry
        # so for now submitting one event at a time.
        self.splunk_index.submit(json_event + '\n', source=source, sourcetype=sourcetype)

      # Need a line break to force each send operation to result in a seperate entry
      # splunk_socket.send(json_event + "\r\n")
      # Try to slow down the speed that events are written to the socket to avoid 
      # multiple events getting munged together.
      # time.sleep(0.5)
      # print json.dumps(json_event)


class SplunkEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, datetime):
      return obj.isoformat()

    return json.JSONEncoder.default(self, obj)

