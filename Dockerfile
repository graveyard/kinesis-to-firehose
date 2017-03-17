FROM openjdk:7

# install `make`
RUN apt-get -y update && apt-get install -y -q build-essential

# install go
RUN wget https://storage.googleapis.com/golang/go1.8.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.8.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin
ENV GOPATH $HOME/go

# add kinesis consumer code
RUN mkdir -p $GOPATH/src/github.com/Clever/kinesis-to-firehose
ADD . $GOPATH/src/github.com/Clever/kinesis-to-firehose/
WORKDIR $GOPATH/src/github.com/Clever/kinesis-to-firehose/

# build
RUN make install_deps build download_jars

CMD ["make", "kinesis_consumer"]
