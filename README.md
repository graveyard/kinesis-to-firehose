# kinesis-to-firehose

Consumes records from Kinesis and writes to Firehose.

## Makefile modification
`make generate` depends on `install_deps`, which can be slow. Feel free
to comment that out locally. Just avoid committing it so that people who
clone the repo don't get into an unbuildable state.

(If you get an error about `mocks/mockfirehose.go` being empty, delete
the file, `make install_deps` and `make generate`.)

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
DEPLOY_ENV="local" \
make run
```

This will download the jar files necessary to run the KCL, and then launch the KCL communicating with the consumer binary.

### Running at Clever

You can also use `ark` to run locally, via `ark start --local`.

Note: In addition to output from the process, you may want to run `tail -f /tmp/kcl_stderr` to view more logs written to file.
These logs aren't written to stdout/stderr, since KCL uses those for communication.
