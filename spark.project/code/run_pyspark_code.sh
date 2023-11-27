#!/bin/bash

# Set the path to your Spark home directory
SPARK_HOME=/opt/spark

# Set the PySpark application's main Python script
PYSPARK_SCRIPT=/home/schronakis/spark_project/init_spark_app.py

# Set other Spark configurations (optional)
SPARK_CONF="--master local
            --executor-cores 2
            --driver-memory 2g 
            --executor-memory 2g"

# Use the spark-submit command to run the PySpark application
$SPARK_HOME/bin/spark-submit $PYSPARK_SCRIPT\
    --master local \
	--deploy-mode client \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 2  \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.executor.memoryOverhead=384" 
#    --conf "spark.memory.fraction=0.8" \
#    --conf "spark.memory.storageFraction=0.2"\
#    --jars file1.jar,file2.jar
# $SPARK_CONF
