# kinesis-to-firehose

Consumes records from Kinesis and writes to Firehose.

## Running the Consumer

To build and run the consumer, set required env vars. Then run, `make run`.

Env vars include:
- Kinesis input stream
- Firehose output stream
- logfile
- rate limit (allows throttling the # of records-per-second to read from each Kinesis shard)

``` bash
KINESIS_AWS_REGION=us-west-1 \
KINESIS_STREAM_NAME=kinesis-test \
FIREHOSE_AWS_REGION=us-west-2 \
FIREHOSE_STREAM_NAME=firehose-test \
LOG_FILE=/tmp/kcl_stderr \
RATE_LIMIT=100 \
make run
```

This will download the jar files necessary to run the KCL, and then launch the KCL communicating with the consumer binary.

### Running at Clever

You can also use `ark` to run locally, via `ark start --local`
