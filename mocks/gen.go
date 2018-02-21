package mocks

//go:generate sh -c "$PWD/bin/mockgen -package mocks -source $PWD/vendor/github.com/aws/aws-sdk-go/service/firehose/firehoseiface/interface.go FirehoseAPI > mockfirehose.go"
