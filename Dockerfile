FROM google/cloud-sdk

RUN apt-get update && \
    apt-get install -y python3 python3-pip  && \
    pip3 install pytest && \
    pytest && \
    pip3 install --upgrade --no-cache-dir .

