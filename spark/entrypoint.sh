#!/bin/bash

cat > $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.hadoop.fs.s3a.access.key ${SPARK_MINIO_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key ${SPARK_MINIO_SECRET_KEY}
spark.hadoop.fs.s3a.endpoint http://minio:9000 
spark.hadoop.fs.s3a.region us-east-1 
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem 
spark.hadoop.fs.s3a.path.style.access true 
spark.databricks.delta.retentionDurationCheck.enabled false 
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension 
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.master                           spark://spark-master:7077
spark.eventLog.enabled                 true
spark.eventLog.dir                     /opt/spark/spark-events
spark.history.fs.logDirectory          /opt/spark/spark-events
spark.jars.packages                    io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3
spark.sql.codegen.wholeStage false
spark.ui.prometheus.enabled            true
EOF

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  $SPARK_HOME/sbin/start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  $SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  $SPARK_HOME/sbin/start-history-server.sh
fi
