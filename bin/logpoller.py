# from daemon import DaemonContext
from cust_daemon import Daemon
import lockfile
import logging
import logging.handlers
import cloudwatch_collector
from cloudwatch_collector import CloudWatchCollector
from sqs_collector import SqsCollector
from splunk_transport import SplunkTransport
from multiprocessing import Process
import sys, os.path
import sched
import time
from datetime import datetime as dt
import datetime
import yaml
from nose.tools import set_trace

logger = logging.getLogger('cloud_log_poller')

def main(debug=False):
  # TODO: Use passed in conf file
  with open ("cloud_log_poller.yaml", "r") as conf_file:
    yaml_string = ''.join(conf_file.readlines())

  # Expand environment variables in the YAML string
  yaml_string = os.path.expandvars(yaml_string)
  config_settings = yaml.load(yaml_string)

  # Set configuration defaults
  config_defaults = {'polling_interval': 30}
  config_settings = dict(config_defaults.items() + config_settings.items())

  # Build a scheduler object that will look at absolute times
  scheduler = sched.scheduler(time.time, time.sleep)

  transports = load_transports(config_settings)
  collectors = load_collectors(config_settings, transports)

  def run_collectors():
    for collector in collectors:
      logger.info("Running collector %s" % collector.__class__.__name__)
      collector.run()

    logger.info("Scheduling next log polling job in %s seconds" % config_settings['polling_interval'])
    next_run_time = dt.now() + datetime.timedelta(seconds=config_settings['polling_interval'])
    scheduler.enterabs(time.mktime(next_run_time.timetuple()), 1, run_collectors, ())

  scheduler.enterabs(time.mktime(dt.now().timetuple()), 1, run_collectors, ())
  scheduler.run()


def load_collectors(config, transports):
  collectors = []
  for collector_config in config['collectors']:
    collector_type = None
    if collector_config['type'] == 'cloudwatch':
      collector_type = CloudWatchCollector
    elif collector_config['type'] == 'sqs':
      collector_type = SqsCollector
    else:
      raise RuntimeError("Invalid collector type: %s" % collector_config['type'])

    collectors.append(collector_type(collector_config, transports))

  logger.info("Found %s collectors: %s" % (len(collectors), [c.__class__.__name__ for c in collectors]))
  return collectors

def load_transports(config):
  transports = []
  for transport_config in config['transports']:
    transport_type = None
    if transport_config['type'] == 'splunk':
      transport_type = SplunkTransport
    else:
      raise RuntimeError("Invalid transport type: %s" % transport_config['type'])

    transports.append(transport_type(transport_config))

  return transports

def configure_logging(debug=False):
  # Clear any default loggers
  logger.handlers = []
  
  handler = None
  # In debug mode log to the console
  if debug is True:
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
  # In standard daemon mode log to a rotating file
  else:
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler("cloug_log_poller.log", mode='a', maxBytes=1000000, backupCount=3)
    handler.setLevel(logging.INFO)

  logger.propagate = False
  handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
  logger.addHandler(handler)    


def display_help():
  print "Usage:"
  print "\tcloud_log_poller <command> [options]"
  print ""
  print "Commands:"
  print "\tstart\t\tStart the daemon process"
  print "\tstop\t\tStop the daemon process"
  print "\trestart\t\tRestart the daemon process"
  print "\trun\t\tRun the process in the foreground rather than a daemon"
  print ""
  print "Options:"
  print "\t-c <conf file>\t\tPath to the config yaml file."
  print "\t\t\t\tDefaults to a cloud_log_poller.conf file in the current directory"

class PollDaemon(Daemon):
  def run(self):
    main(False)

if __name__ == '__main__':
  if "help" in sys.argv or "-h" in sys.argv:
    display_help()
    sys.exit()

  debug = "-D" in sys.argv
  configure_logging(debug)

  # If debug mode, run in the foreground rather than as a daemon
  if debug is True:
    try:
      main(debug=False)
    except KeyboardInterrupt:
      sys.exit()
  else:
    # Use a python-daemon context
    daemon = PollDaemon('cloud_log_poller.pid')
    if 'start' in sys.argv:
      logger.info("Starting daemon process")
      daemon.start()
    elif 'stop' in sys.argv:
      logger.info("Stopping daemon process")
      daemon.stop()
    elif 'restart' in sys.argv:
      logger.info("Restarting daemon process")
      daemon.restart()
