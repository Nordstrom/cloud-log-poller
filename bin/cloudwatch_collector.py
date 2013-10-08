import os
import boto.ec2.cloudwatch
import datetime
import logging
import inflection
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


  def process_metrics(self, metric_name, namespace, dimensions, start_time, end_time):
    datapoints = self.cloudwatch.get_metric_statistics(60, start_time, end_time, metric_name, namespace, STATISTICS, dimensions=dimensions)

    metric_display = [namespace, metric_name]
    metric_display.extend(dimensions.values())
    logger.info("Received %s datapoints for Cloudwatch metric %s" % (len(datapoints), ':'.join(metric_display)))

    if len(datapoints) == 0:
      return

    # Append the metric_name onto each datapoint
    for datapoint in datapoints:
      datapoint['metric'] = metric_name

    for transport in self.__transports:   
      transport.send(namespace, 'cloudwatch', datapoints)

  def run(self):
    end_time = dt.utcnow()
    if self.__last_run:
      start_time = self.__last_run + datetime.timedelta(seconds=1)
    else:
      start_time = end_time - datetime.timedelta(seconds=60)

    # Set the start_time for the next run
    self.__last_run = end_time

    logger.info("Running CloudwatchCollector on %s metrics between %s and %s" % 
      (len(self.__config['metrics']), start_time, end_time))

    for metric in self.__config['metrics']:
      # TODO: Support for dimensions for dynamo table names
      # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/MonitoringDynamoDB.html
      namespace = metric['namespace']
      metric_name = metric['metric_name']

      # Collect the remaining metric dimensions besides metric_name and namespace
      dimensions = {}
      for key in metric:
        if not key in ('metric_name', 'namespace'):
          dimensions[inflection.camelize(key)] = metric[key]
      
      # If metric_name is a list, process each seperately with the same values for namespace and dimensions
      if isinstance(metric_name, list):
        [self.process_metrics(name, namespace, dimensions, start_time, end_time) for name in metric_name]
      else:
        self.process_metrics(metric_name, namespace, dimensions, start_time, end_time)
