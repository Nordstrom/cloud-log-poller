import os
import boto.ec2.cloudwatch
import datetime
import logging
from datetime import datetime as dt
from dateutil import tz

STATISTICS = ['Minimum', 'Maximum', 'Sum', 'Average', 'SampleCount']
logger = logging.getLogger('cloud_log_poller')

class CloudWatchCollector():
	def __init__(self, config, transport):
		self.config = config
		self.transport = transport

		aws_key_id = os.environ['AWS_ACCESS_KEY_ID']
		aws_secret_key= os.environ['AWS_SECRET_ACCESS_KEY']

		self.cloudwatch = boto.ec2.cloudwatch.connect_to_region(
	    self.config['aws_region'], 
	    aws_access_key_id=aws_key_id,
	    aws_secret_access_key=aws_secret_key)

		self.local_zone = tz.tzlocal()
		self.utc_zone = tz.tzutc()

		if 'cloudwatch_polling_interval' in config:
			self.polling_interval = config['cloudwatch_polling_interval'] 
		else:
			self.polling_interval = 300


	def run(self):
		logger.debug("Running CloudwatchCollector")

		end_time = dt.utcnow()
		start_time = end_time - datetime.timedelta(seconds=self.polling_interval)
		events = []

		for metric in self.config['cloudwatch_metrics']:
			namespace, metric_name = metric.split(':')
			datapoints = self.cloudwatch.get_metric_statistics(60, start_time, end_time, metric_name, namespace, STATISTICS)
			for datapoint in datapoints:
				utc_time = datapoint['Timestamp'].replace(tzinfo=self.utc_zone)
				local_time = utc_time.astimezone(self.local_zone)

				datapoint['Timestamp'] = local_time.strftime("%m/%d/%y %I:%M:%S.000 %p")
				datapoint['MetricName'] = metric_name
				events.append(datapoint)
		
			logger.debug("Found %s CloudWatch data points" % len(events))
			self.transport.send(namespace, 'cloudwatch', events)