FROM openjdk:7

# install `make`
RUN apt-get -y update && apt-get install -y -q build-essential
ADD golang.mk .
ADD Makefile .
RUN make download_jars

# build
ADD ./build/consumer ./build/consumer

CMD ["make", "kinesis_consumer"]
