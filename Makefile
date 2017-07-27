include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
JAR_DIR := jars
PKG := github.com/Clever/kinesis-to-firehose
PKGS := $(shell go list ./... | grep -v /vendor | grep -v /writer/mock_firehoseiface)
.PHONY: download_jars run build

KCL_VERSION := 1.7.2

define POM_XML_FOR_GETTING_DEPENDENT_JARS
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.clever.kinesistofirehose</groupId>
  <artifactId>kinesis-to-firehose</artifactId>
  <version>1.0-SNAPSHOT</version>
  <dependencies>
    <dependency>
      <groupId>com.amazonaws</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>$(KCL_VERSION)</version>
    </dependency>
  </dependencies>
</project>
endef
export POM_XML_FOR_GETTING_DEPENDENT_JARS
download_jars:
	command -v mvn >/dev/null 2>&1 || { echo >&2 "Maven not installed. Install maven!"; exit 1; }
	mkdir -p /tmp/kinesis-to-firehose
	echo $$POM_XML_FOR_GETTING_DEPENDENT_JARS > /tmp/kinesis-to-firehose/pom.xml
	cd /tmp/kinesis-to-firehose && mvn dependency:copy-dependencies
	cp /tmp/kinesis-to-firehose/target/dependency/* $(JAR_DIR)/

all: test build

build:
	CGO_ENABLED=0 go build -installsuffix cgo -o build/consumer $(PKG)/cmd/consumer

$(GOPATH)/bin/glide:
	@go get github.com/Masterminds/glide

install_deps: $(GOPATH)/bin/glide
	@$(GOPATH)/bin/glide install


KINESIS_STREAM_NAME ?= logs-test
KINESIS_AWS_REGION ?= us-west-1
# sets the `applicationName` in KCL consume.properties.
# we want a dif application for local, dev, and prod, so that they don't conflict
# (e.g. app name determines the DynamoDB table that KCL uses to coordinate)
KINESIS_APPLICATION_NAME ?= kinesis-to-firehose-local

consumer_properties:
	cp consumer.properties.template consumer.properties
	sed -i 's/<STREAM_NAME>/$(KINESIS_STREAM_NAME)/' consumer.properties
	sed -i 's/<REGION_NAME>/$(KINESIS_AWS_REGION)/' consumer.properties
	sed -i 's/<APPLICATION_NAME>/$(KINESIS_APPLICATION_NAME)/' consumer.properties
	sed -i 's/<INITIAL_POSITION>/$(KINESIS_INITIAL_POSITION)/' consumer.properties

run_kinesis_consumer: consumer_properties
	command -v java >/dev/null 2>&1 || { echo >&2 "Java not installed. Install java!"; exit 1; }
	java -cp "$(JAR_DIR)/*"  com.amazonaws.services.kinesis.multilang.MultiLangDaemon consumer.properties

run:
	GOOS=linux GOARCH=amd64 make build
	docker build -t kinesis-to-firehose .
	@docker run -v /tmp:/tmp --env-file=<(echo -e $(_ARKLOC_ENV_FILE)) kinesis-to-firehose

test: $(PKGS)
$(PKGS): golang-test-all-strict-deps
	$(call golang-test-all-strict,$@)

bench:
	go test -bench=. github.com/Clever/kinesis-to-firehose/decode/
