from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, StructType, StructField, BooleanType, DataType

dwell_data_schema = StructType([
    StructField("Year", IntegerType()),
    StructField("DwellRecType", IntegerType()),
    StructField("DwellStatus", IntegerType()),
    StructField("Area", IntegerType()),
    StructField("Count", IntegerType())
    ])

dwell_area_schema = StructType([
    StructField("Code", IntegerType()),
    StructField("Description", StringType()),
    StructField("SortOrder", IntegerType())
    ])
