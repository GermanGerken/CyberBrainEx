FROM apache/airflow:2.3.2
# this fixes a warning in the official image, with the Azure provider
# More info here https://github.com/apache/airflow/issues/14266#issuecomment-786298240
#RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
# add your dependencies here:
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;



# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
USER airflow
RUN pip install pyspark~=3.2.1