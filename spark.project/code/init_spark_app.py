from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, broadcast, col

spark = SparkSession.builder\
        .appName('InitApp') \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.cores.max", "4") \
        .getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp','Python'],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df_explode = df.select(df.name,explode(df.knownLanguages))
# df_explode.printSchema()
# df_explode.show()

################################### RDD Broadcast ############################################

states = {"NY":"New York", "CA":"California", "FL":"Florida"}

broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()

# print((result))

################################ DataFrame Broadcast #########################################

columns = ["firstname","lastname","country","state"]

df = spark.createDataFrame(data = data, schema = columns)

# df.printSchema()
# df.show(truncate=False)

def state_convert(code):
    return broadcastStates.value[code]

result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)

# result.show(truncate=False)

##############################################################################################

################################ DataFrame Broadcast #########################################

#### Create a Larger DataFrame using parquet Dataset
# largeDF = spark.read \
#         .option("header",True) \
#         .option("inferschema",True) \
#         .parquet("dbfs:/mnt/training/weather/StationData/stationData.parquet") \
#         .limit(2000)

large_df_columns = ["code","firstname","lastname","country","state"]

large_data = [("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL"),
              ("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL"),
              ("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL"),
              ("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL"),
              ("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL"),
              ("1","James","Smith","USA","CA"),
              ("2","Michael","Rose","USA","NY"),
              ("3","Robert","Williams","USA","TX"),
              ("4","Maria","Jones","USA","FL")]

large_df = spark.createDataFrame(data = large_data, schema = large_df_columns)

#Create a smaller dataFrame with abbreviation of codes
simpleData = (("CA", "Canada"),
              ("NY", "New York"),
              ("FL", "Florida"),
              ("TX", "Texas"))

small_df = spark.createDataFrame(data = simpleData, schema = ["country_code", "realUnit"])

joined_df = large_df.join(broadcast(small_df), 
                          large_df["state"] == small_df["country_code"])\
                                .select(large_df['code'],
                                        col('state'),
                                        col('realUnit')) #.explain(extended=False)
large_df.show()
small_df.show()
joined_df.show()

# joined_df.show()