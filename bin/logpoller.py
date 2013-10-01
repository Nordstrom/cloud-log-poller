from daemon import DaemonContext
import logging
import cloudwatch_collector
from cloudwatch_collector import CloudWatchCollector
from sqs_collector import SqsCollector
from splunk_transport import SplunkTransport
import sys
import sched
import time
from datetime import datetime as dt
import datetime
import yaml

logger = logging.getLogger('cloud_log_poller')

def now_str():
    """Return hh:mm:ss string representation of the current time."""
    t = dt.now().time()
    return t.strftime("%H:%M:%S")

def main():
  stream = open("config.yaml", 'r')
  config_settings = yaml.load(stream)
  configure_logging()

  def run_collector(collector):
    collector.run()

    logger.debug("Scheduling next collector run in %s seconds" % collector.polling_interval)
    next_run_time = dt.now() + datetime.timedelta(seconds=collector.polling_interval)
    scheduler.enterabs(time.mktime(next_run_time.timetuple()), 1, run_collector, (collector,))

  # Build a scheduler object that will look at absolute times
  scheduler = sched.scheduler(time.time, time.sleep)
  transport = SplunkTransport(config_settings)

  # The list of collectors to gather log events from
  collectors = [
    CloudWatchCollector(config_settings, transport), 
    SqsCollector(config_settings, transport)
  ]

  for collector in collectors:
    # time, priority, callable, *args
    scheduler.enterabs(time.mktime(dt.now().timetuple()), 1,
                       run_collector, (collector,))

  scheduler.run()


def configure_logging():
  logger.setLevel(logging.DEBUG)
  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setLevel(logging.DEBUG)
  formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)


if __name__ == '__main__':
  if "-f" in sys.argv:
    try:
      main()
    except KeyboardInterrupt:
      sys.exit()
  else:
    # Use a python-daemon context
    with DaemonContext():
      main()