# Custom Jenkins Dockerfile with Spark/Databricks CLI
FROM jenkins/jenkins:lts

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    openjdk-17-jdk \
    curl \
    wget \
    git \
    unzip \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install --no-cache-dir \
    databricks-cli \
    pyspark==3.5.0 \
    kafka-python-ng \
    pandas \
    boto3 \
    python-dotenv \
    mlflow \
    databricks-sql-connector \
    librosa \
    scikit-learn

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/bin/python3

# Install Jenkins plugins
RUN jenkins-plugin-cli --plugins \
    git \
    workflow-aggregator \
    docker-workflow \
    blueocean \
    credentials-binding \
    slack \
    email-ext \
    timestamper \
    ws-cleanup

# Create directory for Databricks configuration
RUN mkdir -p /var/jenkins_home/.databrickscfg

USER jenkins

# Set working directory
WORKDIR /var/jenkins_home
