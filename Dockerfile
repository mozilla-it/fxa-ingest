FROM google/cloud-sdk

COPY . /workspace/

WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y python3 python3-pip  && \
    pip3 install pytest && \
    pip3 install --upgrade --no-cache-dir . && \
    pytest && \
    apt-get clean

