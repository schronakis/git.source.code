from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, StructType, StructField, BooleanType, DateType

init_client_schema = StructType([
    StructField("client_id", IntegerType()),
    StructField("birth_number", IntegerType()),
    StructField("district_id", IntegerType())
    ])

complete_client_schema = StructType([
    StructField("client_id", StringType()),
    StructField("sex", StringType()),
    StructField("fulldate", DateType()),
    StructField("day", IntegerType()),
    StructField("month", IntegerType()),
    StructField("year", IntegerType()),
    StructField("age", IntegerType()),
    StructField("social", StringType()),
    StructField("first", StringType()),
    StructField("middle", StringType()),
    StructField("last", StringType()),
    StructField("phone", StringType()),
    StructField("email", StringType()),
    StructField("address_1", StringType()),
    StructField("address_2", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zipcode", StringType()),
    StructField("district_id", IntegerType())
    ])

mock_df_schema = StructType([
    StructField("client_id", StringType()),
    StructField("fullname", StringType()),
    StructField("birthday_date", StringType()),
    StructField("social_number", StringType()),
    StructField("phone", StringType()),
    StructField("email", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("zipcode", IntegerType()),
    StructField("district_id", IntegerType())
    ])
