#!/bin/bash
source configs

SLAVES=/opt/spark/conf/slaves
# /opt/hadoop/bin/hdfs dfs -copyFromLocal ~/datasets/$GRAPH_FILE /
for NUM_PARITIONS in `seq $START $STEP $END`
do

python mysar.py start $SLAVES
  # HDFS
# /usr/bin/time -f "%e" timeout $TIMEOUT /opt/spark/bin/spark-submit --master spark://$PRIVATE_MASTER_IP:7077 --executor-memory $EXECUTOR_MEMORY \
# --class $APP ../graphx-examples/target/scala-2.11/graphx-examples_2.11-1.0.jar \
# hdfs://$PRIVATE_MASTER_IP:9000/$GRAPH_FILE $NUM_PARITIONS > ${APP}_${VM_TYPE}_${N}_${NUM_PARITIONS}.log 2>&1

  # S3
/usr/bin/time -f "%e" timeout $TIMEOUT /opt/spark/bin/spark-submit \
--driver-memory 6g --master spark://$PRIVATE_MASTER_IP:7077 --executor-memory $EXECUTOR_MEMORY \
--class $APP graphx-examples_2.11-1.0.jar \
s3a://graphanalytics-datasets/$GRAPH_FILE $NUM_PARITIONS > ${APP}_${VM_TYPE}_${N}_${NUM_PARITIONS}.log 2>&1

./collectSar.sh $SLAVES ${APP}_${VM_TYPE}_${N}_${NUM_PARITIONS}
done
