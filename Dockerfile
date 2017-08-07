FROM openjdk:7-jre

# install `make`
RUN apt-get -y update && apt-get install -y -q build-essential

ADD jars jars
ADD consumer.properties.template .
ADD run_kcl.sh .
ADD kinesis-to-firehose kinesis-to-firehose

ENTRYPOINT ["/bin/bash", "./run_kcl.sh"]
