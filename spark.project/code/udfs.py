import pyspark.sql.functions as F
from pyspark.sql.functions import udf 
from spark_session import sparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import pandas_udf, PandasUDFType

sc = sparkSession('generate_pdf','2g','2g','4')
spark_session = sc.generate_spark_session()

partition_size = spark_session.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")

print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")
print(f"Parallelism : {spark_session.sparkContext.defaultParallelism}")

# Enable Arrow-based columnar data transfers
spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Define UDFs
def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
       resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr
 
def upperCase(str):
    return str.upper()

@pandas_udf('integer') # , PandasUDFType.SCALAR
def squared(v):
    return v * v

# Register the UDF with Spark
convertUDF = udf(lambda z: convertCase(z), StringType())
upperCaseUDF = udf(upperCase, StringType())

# Create a dataframe with columns of strings
columns = ["Seqno","Name"]

data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders")]

# df = spark_session.createDataFrame(data=data,schema=columns)

# df.withColumn("Cureated Name", upperCaseUDF(F.col("Name"))) \
#   .show(truncate=False)

# df.select(F.col("Seqno"), \
#     convertUDF(F.col("Name")).alias("Name") ) \
#    .show(truncate=False)

x = [1, 2, 3]

df = spark_session.createDataFrame(x, IntegerType())

df.select(squared(F.col('value'))).explain()
