import os
import json
import logging
import splunklib.client as splunk_client

logger = logging.getLogger('cloud_log_poller')

class SplunkTransport:
	def __init__(self, config):
		self.config = config

		splunk_username = os.environ['SPLUNK_USERNAME']
		splunk_password = os.environ['SPLUNK_PASSWORD']

		self.splunk_service = splunk_client.connect(
	    host=config['splunk_host'],
	    port=config['splunk_port'],
	    username=splunk_username,
	    password=splunk_password)

		self.splunk_index = self.splunk_service.indexes[config['splunk_index']]

	def send(self, source, sourcetype, log_events):
		logger.debug("Sending %s events to Splunk, source=%s, sourcetype=%s" 
			% (len(log_events), source, sourcetype))

		with self.splunk_index.attached_socket(source=source, sourcetype=sourcetype) as splunk_socket:
			for event in log_events:
				# TODO: Convert event attribute names to underscore format

				json_event = json.dumps(event)
				# Need a line break to force each send operation to result in a seperate entry
				splunk_socket.send(json_event + "\r\n")
				# print json.dumps(json_event)
