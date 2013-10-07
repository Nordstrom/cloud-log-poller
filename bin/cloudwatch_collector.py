import os
import boto.ec2.cloudwatch
import datetime
import logging
from datetime import datetime as dt

STATISTICS = ['Minimum', 'Maximum', 'Sum', 'Average', 'SampleCount']
logger = logging.getLogger('cloud_log_poller')

class CloudWatchCollector():
  def __init__(self, config, transports):
    self.__config = config
    self.__transports = transports
    self.__last_run = None

    self.cloudwatch = boto.ec2.cloudwatch.connect_to_region(
      config['aws_region'], 
      aws_access_key_id=config['aws_access_key_id'],
      aws_secret_access_key=config['aws_secret_access_key'])


  def run(self):
    logger.info("Running CloudwatchCollector on %s metrics" % len(self.__config['metrics']))

    end_time = dt.utcnow()
    if self.__last_run:
      start_time = self.__last_run + datetime.timedelta(seconds=1)
    else:
      start_time = end_time - datetime.timedelta(seconds=60)

    # Set the start_time for the next run
    self.__last_run = end_time

    events = []

    for metric in self.__config['metrics']:
      namespace, metric_name = metric.split(':')
      datapoints = self.cloudwatch.get_metric_statistics(60, start_time, end_time, metric_name, namespace, STATISTICS)
     
      for datapoint in datapoints:
        logger.debug("Cloudwatch datapoint: " + repr(datapoint))
        events.append(datapoint)

    logger.info("Found %s Cloudwatch datapoints" % (len(events)))    
    if (len(events) > 0):
      for transport in self.__transports:   
        transport.send(namespace, 'cloudwatch', events)
