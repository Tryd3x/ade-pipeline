FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.4.4
ARG jupyterlab_version=4.1.5
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y wget && \
    pip3 install --no-cache-dir \
        pyspark==${spark_version} \
        jupyterlab==${jupyterlab_version} && \
    rm -rf /var/lib/apt/lists/*

# -- Runtime
WORKDIR /opt/workspace
