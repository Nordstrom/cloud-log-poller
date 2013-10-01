import logging

logger = logging.getLogger('cloud_log_poller')

class SqsCollector:
  def __init__(self, config, transport):
    self.__config = config
    self.__transport = transport

  def run(self):
    logger.debug("Running SqsCollector")