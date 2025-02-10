FROM deltaio/delta-docker:latest

USER root

# Installation des dépendances nécessaires pour Spark et TPC-H
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    rsync \
    git \
    make \
    gcc \
    postgresql-client \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen && \
    cd /opt/tpch-dbgen && \
    make clean && \
    make

RUN mkdir -p /opt/tpch-data

WORKDIR /opt/tpch-dbgen
RUN ./dbgen -s 1 && \
    mv *.tbl /opt/tpch-data/
    
# Créer les dossiers nécessaires
RUN mkdir -p /opt/spark/tests /opt/spark/notebooks /tmp/spark-events

# Copier les fichiers
#COPY tests/test-minio.py /opt/spark/tests
COPY ./requirements.txt ./
COPY ./start-jupyter.sh /opt/spark/scripts/
COPY conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY conf/metrics.properties "$SPARK_HOME/conf/metrics.properties"

# Installation des dépendances Python
RUN pip install -r requirements.txt

# Configuration des permissions
RUN chmod +x /opt/spark/scripts/start-jupyter.sh && \
    chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Configuration des variables d'environnement
ENV SPARK_CONF_DIR="$SPARK_HOME/conf" \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_MASTER_HOST=spark-master \
    SPARK_MASTER_PORT=7077 \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copie et configuration du script d'entrée
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]