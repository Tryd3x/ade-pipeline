FROM openjdk:11-slim

ARG shared_workspace=/opt/workspace
ENV SHARED_WORKSPACE=${shared_workspace}

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        zip \
        unzip \
        wget \
        curl \
        procps \
        && \
    mkdir -p ${SHARED_WORKSPACE} && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# -- Runtime
CMD ["bash"]