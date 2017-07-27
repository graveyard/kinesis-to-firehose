FROM openjdk:7-jre

# install make and mvn
RUN apt-get -y update && apt-get install -y -q build-essential maven
ADD golang.mk .
ADD Makefile .
RUN make download_jars

# build
ADD consumer.properties.template .
ADD ./build/consumer ./build/consumer

CMD ["make", "run_kinesis_consumer"]
