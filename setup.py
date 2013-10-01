#!/usr/bin/env python
from setuptools import setup, find_packages
from bin import VERSION

setup(name='cloud_log_poller',
  version=VERSION,
  url='https://github.com/Nordstrom/cloud-log-poller',
  author="Nordstrom Data Lab",
  author_email="ds@nordstrom.com",
  description="A command-line daemon for polling cloud log sources and sending collected log events to a transport such as Splunk.",
  long_description=open('README.rst').read(),
  packages=find_packages(),
  scripts=['bin/logpoller.py'])