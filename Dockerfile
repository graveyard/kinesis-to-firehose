FROM openjdk:8-jre

WORKDIR /

# install `make`
RUN apt-get -y update && apt-get install -y -q build-essential

ADD jars jars
ADD consumer.properties.template .
ADD run_kcl.sh .
ADD kvconfig.yml .
ADD kinesis-consumer kinesis-consumer

ENTRYPOINT ["/bin/bash", "./run_kcl.sh"]
