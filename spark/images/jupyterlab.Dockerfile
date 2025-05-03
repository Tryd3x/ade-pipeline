FROM cluster-base

ARG spark_version=3.4.4
ARG jupyterlab_version=4.1.5

RUN pip3 install --no-cache-dir \
        pyspark==${spark_version} \
        jupyterlab==${jupyterlab_version}

WORKDIR ${SHARED_WORKSPACE}
