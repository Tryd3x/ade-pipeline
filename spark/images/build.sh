# -- Software Stack Version

SPARK_VERSION="3.4.4"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="4.1.5"
SHARED_WORKSPACE="/opt/workspace"
LIVY_HOME="/opt/livy"
LIVY_VERSION="0.8.0-incubating"

# -- Building the Images

docker build \
  --build-arg shared_workspace="${SHARED_WORKSPACE}" \
  -f cluster-base.Dockerfile \
  -t cluster-base .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f spark-base.Dockerfile \
  -t spark-base .

docker build \
  -f spark-master.Dockerfile \
  -t spark-master .

docker build \
  -f spark-worker.Dockerfile \
  -t spark-worker .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t jupyterlab .

docker build \
  --build-arg livy_version="${LIVY_VERSION}" \
  --build-arg livy_home="${LIVY_HOME}" \
  -f livy.Dockerfile \
  -t livy .