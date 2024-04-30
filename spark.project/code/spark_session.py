from pyspark.sql import SparkSession

class sparkSession:

    def __init__(self, app_name, executor_memory, driver_memory, cores_max):

        self.app_name = app_name
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.cores_max = cores_max

    def generate_spark_session(self):  
        # .config("spark.jars", "mssql-jdbc-12.4.2.jre11.jar") \
        spark = SparkSession.builder\
                .appName(self.app_name) \
                .config("spark.executor.memory", self.executor_memory) \
                .config("spark.driver.memory", self.driver_memory) \
                .config("spark.cores.max", self.cores_max) \
                .config("spark.sql.adaptive.enabled", True) \
                .config("spark.sql.adaptive.optimizerEnabled", True) \
                .config("spark.sql.adaptive.skewJoin.enabled", True) \
                .config('spark.submit.pyFiles', '/home/schronakis/git.source.code/spark.project/code/utils.zip') \
                .getOrCreate()
        
        return spark
