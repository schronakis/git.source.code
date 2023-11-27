from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


spark_conf = SparkConf()
#Define custom configuration properties
spark_conf.set("spark.executor.memory", "2g")
spark_conf.set("spark.executor.cores", "4")


context = SparkContext(conf=spark_conf)
#Define custom context properties
context.setCheckpointDir("checkpoints")

spark = (SparkSession(context)
         .builder
         .appName("DefaultSparkSession")
         .getOrCreate())

print(spark.sparkContext._conf.getAll())