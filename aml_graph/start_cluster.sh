#!/bin/bash

$SPARK_HOME/sbin/start-master.sh

export MASTER=spark://limmen
export SPARK_WORKER_INSTANCES=1
export CORES_PER_WORKER=4

$SPARK_HOME/sbin/start-slave.sh $MASTER:7077 --cores 4 --memory 25g

firefox http://127.0.0.1:8080 &
