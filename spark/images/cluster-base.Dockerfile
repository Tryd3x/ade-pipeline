ARG debian_bullseye_image_tag=11-slim
FROM openjdk:${debian_bullseye_image_tag}

# -- Layer: OS + Python 3.7
RUN mkdir -p /opt/workspace && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip zip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# -- Runtime
CMD ["bash"]