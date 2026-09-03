FROM apache/spark:4.1.1-python3

USER root

# Dependencies for downloading and building
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# Download required JAR packages
ENV ICEBERG_VERSION=1.11.0
ENV HADOOP_AWS_VERSION=3.4.2
ENV AWS_SDK_V2_VERSION=2.25.64
ENV PG_VERSION=42.7.3
ENV COMMONS_POOL_VERSION=2.11.1

RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/aws-sdk-v2-bundle-${AWS_SDK_V2_VERSION}.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/postgresql-${PG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_VERSION}/postgresql-${PG_VERSION}.jar \
 && curl -fsSL -o /opt/spark/jars/commons-pool2-${COMMONS_POOL_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar

USER spark
WORKDIR /opt/spark/work-dir
