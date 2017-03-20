# kinesis-to-firehose

Consumes records from Kinesis and writes to Firehose.

## Running the Consumer

Build the consumer binary:

``` bash
make build
```

Then run, setting env vars as appropriate for your Kinesis input stream and Firehose output stream.

``` bash
KINESIS_AWS_REGION=us-west-1 \
KINESIS_STREAM_NAME=kinesis-test \
FIREHOSE_AWS_REGION=us-west-2 \
FIREHOSE_STREAM_NAME=firehose-test \
make run
```

This will download the jar files necessary to run the KCL, and then launch the KCL communicating with the consumer binary.
