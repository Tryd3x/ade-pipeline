FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.1
ARG jupyterlab_version=3.6.1

# RUN apt-get update -y && \
#     apt-get install -y python3-pip && \
#     pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

RUN apt-get update -y && \
    apt-get install -y python3-pip python3.12-venv && \
    python3 -m venv /env && \
    /env/bin/pip install --upgrade pip && \
    /env/bin/pip install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

    
# -- Runtime    
EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}

ENV PATH="/env/bin:$PATH"

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]
