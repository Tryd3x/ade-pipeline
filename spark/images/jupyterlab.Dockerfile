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
EXPOSE 8888
WORKDIR /opt/workspace

# CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=", "--NotebookApp.disable_check_xsrf=True"]
