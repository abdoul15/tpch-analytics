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



# Création du répertoire projet
RUN mkdir -p /opt/spark/project

# Installation du projet Python
WORKDIR /opt/spark/project
COPY pyproject.toml .
RUN pip install --upgrade pip && \
    pip install ".[dev]"

# Installation et configuration de TPC-H
WORKDIR /opt/tpch-dbgen
RUN git clone https://github.com/electrum/tpch-dbgen.git . && \
    make clean && \
    make

RUN mkdir -p /opt/tpch-data
RUN ./dbgen -s 0.01 && \
    mv *.tbl /opt/tpch-data/
    
# Créer les dossiers nécessaires
RUN mkdir -p /opt/spark/tests /opt/spark/notebooks /tmp/spark-events /opt/spark/scripts

# Copier les fichiers
COPY start-jupyter.sh /opt/spark/scripts/
COPY conf/metrics.properties "$SPARK_HOME/conf/metrics.properties"

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
    PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH \
    PYTHONPATH="/opt/spark/project:${SPARK_HOME}/python:${PYTHONPATH}"

# Copie et configuration du script d'entrée
WORKDIR /opt/spark/project
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]