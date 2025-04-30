FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.4.4
ARG jupyterlab_version=4.1.5

RUN apt-get update -y && \
    apt-get install -y wget && \
    pip3 install \
        pyspark==${spark_version} \
        jupyterlab==${jupyterlab_version}

# -- Runtime
        
EXPOSE 8888
WORKDIR /opt/workspace

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=", "--NotebookApp.disable_check_xsrf=True"]
