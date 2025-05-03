FROM spark-base

ARG livy_version=0.8.0-incubating
ARG livy_home=/opt/livy

ENV LIVY_VERSION=${livy_version}
ENV LIVY_HOME=${livy_home}
ENV LIVY_LOG_DIR=${LIVY_HOME}/logs

# Add livy/bin to PATH variable
ENV PATH=${LIVY_HOME}/bin:${PATH}

# Install Livy
RUN wget https://downloads.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip && \
    unzip apache-livy-${LIVY_VERSION}_2.12-bin.zip && \
    mv apache-livy-${LIVY_VERSION}_2.12-bin ${LIVY_HOME} && \
    rm apache-livy-${LIVY_VERSION}_2.12-bin.zip

# Logs and Config dirs
RUN mkdir -p ${LIVY_HOME}/logs ${LIVY_HOME}/conf

# Create log4j.properties
RUN echo "log4j.rootLogger=INFO, console" > ${LIVY_HOME}/conf/log4j.properties && \
    echo "log4j.appender.console=org.apache.log4j.ConsoleAppender" >> ${LIVY_HOME}/conf/log4j.properties && \
    echo "log4j.appender.console.target=System.out" >> ${LIVY_HOME}/conf/log4j.properties && \
    echo "log4j.appender.console.layout=org.apache.log4j.PatternLayout" >> ${LIVY_HOME}/conf/log4j.properties && \
    echo 'log4j.appender.console.layout.ConversionPattern=%d{ISO8601} [%t] %-5p %c - %m%n' >> ${LIVY_HOME}/conf/log4j.properties