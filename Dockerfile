FROM apache/airflow:2.7.1-python3.10

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
