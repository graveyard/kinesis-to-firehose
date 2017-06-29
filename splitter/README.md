splitter
===

Splitter takes input from a CloudWatchLogs subscription, and splits it into multiple logs.

These logs are modified to mimic the RSyslog format we expect from our other logs. This allows them to be decoded normally by the rest of the pipeline.


