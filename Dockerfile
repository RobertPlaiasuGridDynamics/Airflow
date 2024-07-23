FROM apache/airflow:2.9.2

# Install JDK
USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt