# kinesis-to-firehose

Consumes records from Kinesis and writes to Firehose.

## Running the Consumer

To build and run the consumer, set required env vars for your Kinesis input stream, Firehose output stream, and logfile. Then run `make run`.

``` bash
KINESIS_AWS_REGION=us-west-1 \
KINESIS_STREAM_NAME=kinesis-test \
FIREHOSE_AWS_REGION=us-west-2 \
FIREHOSE_STREAM_NAME=firehose-test \
LOG_FILE=/tmp/kcl_stderr \
make run
```

This will download the jar files necessary to run the KCL, and then launch the KCL communicating with the consumer binary.

### Running at Clever

You can also use `ark` to run locally, via `ark start --local`
