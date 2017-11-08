include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
PKG := github.com/Clever/kinesis-to-firehose
PKGS := $(shell go list ./... | grep -v /vendor | grep -v /writer/mock_firehoseiface)
.PHONY: download_jars run build
$(eval $(call golang-version-check,1.9))

TMP_DIR := ./tmp-jars
JAR_DIR := ./jars/
KCL_VERSION := 1.7.6

define POM_XML_FOR_GETTING_DEPENDENT_JARS
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.clever.kinesistofirehose</groupId>
  <artifactId>kinesis-consumer</artifactId>
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
	mkdir -p $(JAR_DIR) $(TMP_DIR)
	echo $$POM_XML_FOR_GETTING_DEPENDENT_JARS > $(TMP_DIR)/pom.xml
	cd $(TMP_DIR) && mvn dependency:copy-dependencies
	mv $(TMP_DIR)/target/dependency/* $(JAR_DIR)/
	# Download the STS jar file for supporting IAM Roles
	ls $(JAR_DIR)/aws-java-sdk-core-*.jar | sed -e "s/.*-sdk-core-//g" | sed -e "s/\.jar//g" > /tmp/version.txt
	curl -o $(JAR_DIR)/aws-java-sdk-sts-`cat /tmp/version.txt`.jar http://central.maven.org/maven2/com/amazonaws/aws-java-sdk-sts/`cat /tmp/version.txt`/aws-java-sdk-sts-`cat /tmp/version.txt`.jar
	rm -r $(TMP_DIR)

all: test build

build:
	CGO_ENABLED=0 go build -a -installsuffix cgo -o kinesis-consumer



run:
	GOOS=linux GOARCH=amd64 make build
	docker build -t kinesis-to-firehose .
	@docker run -v /tmp:/tmp --env-file=<(echo -e $(_ARKLOC_ENV_FILE)) kinesis-to-firehose

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)
