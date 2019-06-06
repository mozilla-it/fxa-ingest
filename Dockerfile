FROM google/cloud-sdk

RUN apt-get update && apt-get install python3                         \
    && apt-get install python3-pip -y                                 \
    && pip3 install git+https://github.com/mozilla-it/fxa-ingest
