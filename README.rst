cloud-log-poller
================

A command-line daemon for polling cloud log sources like AWS and sending collected log events to a transport such as Splunk. Useful when logging sources exist on a public cloud but you want to store and report on aggregted logs from an on-premise repository. In this case it is not possible to push logs from the cloud using standard log forwarding. Includes two built-in log sources: AWS CloudWatch and Simple Queue Service. 