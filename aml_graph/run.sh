#!/bin/bash

##$SPARK_HOME/bin/spark-submit \
##    --master spark://limmen-MS-7823:7077 \
##    --class "limmen.fasttext_on_spark.Main" \
##    --conf spark.cores.max=8 \
##    --conf spark.task.cpus=7 \
##    --conf spark.executorEnv.JAVA_HOME="$JAVA_HOME" \
##    --conf spark.rpc.message.maxSize=2000 \
##    --executor-memory 8g \
##    --driver-memory 8g \

java -jar /home/kim/workspace/python/synthethic_AML_detection/aml_graph/target/scala-2.11/aml_graph-assembly-0.1.0-SNAPSHOT.jar  --input "/home/kim/workspace/python/synthethic_AML_detection/data/cleaned_transactions.csv" --output "/home/kim/workspace/python/synthethic_AML_detection/data/output"
