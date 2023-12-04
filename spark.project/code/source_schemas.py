from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, StructType, StructField, BooleanType, DataType

dwell_data_schema = StructType([
    StructField("Year", IntegerType()),
    StructField("DwellRecType", IntegerType()),
    StructField("DwellStatus", IntegerType()),
    StructField("Area", IntegerType()),
    StructField("Count", IntegerType())
    ])

dwell_dim_schema = StructType([
    StructField("Code", IntegerType()),
    StructField("Description", StringType()),
    StructField("SortOrder", IntegerType())
    ])

mock_df_schema = StructType([
    StructField("Year", IntegerType()),
    StructField("DwellRecType", IntegerType()),
    StructField("DwellStatus", IntegerType()),
    StructField("Area", IntegerType()),
    StructField("Count", IntegerType()),
    StructField("Year_Count", StringType())
    ])