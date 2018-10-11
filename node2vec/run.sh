#!/bin/bash

$SPARK_HOME/bin/spark-submit \
    --master spark://limmen:7077 \
    --class "limmen.github.com.node2vec.Main" \
    --conf spark.executorEnv.JAVA_HOME="$JAVA_HOME" \
    --conf spark.rpc.message.maxSize=2000 \
    --executor-memory 32g \
    --conf spark.cores.max=4 \
    --conf spark.task.cpus=4 \
    /home/kim/workspace/python/synthethic_AML_detection/node2vec/target/scala-2.11/node2vec-assembly-0.1.0-SNAPSHOT.jar \
    --input "/home/kim/workspace/python/synthethic_AML_detection/data/output/data/edges" \
    --output "/home/kim/workspace/python/synthethic_AML_detection/data/output/data/node2vec" \
    --cluster --partitions 1 --dim 10 --windowsize 10 --numwalks 10 --p 1.0 --q 1.0 --weighted --degree 30 \
    --indexed --cmd "node2vec" \
    --nodepath "/home/kim/workspace/python/synthethic_AML_detection/data/account_names"

#    --conf spark.cores.max=4 \
#    --conf spark.task.cpus=4 \
#    --executor-memory 8g \
#    --driver-memory 8g \

#java -jar /home/kim/workspace/python/synthethic_AML_detection/aml_graph/target/scala-2.11/aml_graph-assembly-0.1.0-SNAPSHOT.jar  --input "/home/kim/workspace/python/synthethic_AML_detection/data/cleaned_transactions.csv" --output "/home/kim/workspace/python/synthethic_AML_detection/data/output"
