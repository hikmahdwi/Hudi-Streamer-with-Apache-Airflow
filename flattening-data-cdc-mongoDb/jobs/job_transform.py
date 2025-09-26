from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

HUDI_BASE= "s3a://advertising-data-lake/hudi-kuponku/flattened/"
HUDI_TABLE_NAME = "release"
HUDI_PATH = HUDI_BASE + HUDI_TABLE_NAME

spark = SparkSession.builder \
    .appName("Read From Hive Metastore") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .enableHiveSupport() \
    .getOrCreate()

#-------------------------- Read Data From Hive Metastore ------------------------------  

df_sql = spark.sql(f"Select oid, ts_date, ts, values from kuponku_cdc_release")

json_rdd = df_sql.select("values").rdd.map(lambda row: row.values)
inferred_schema = spark.read.json(json_rdd).schema
print("=========================================")
print("===== [DEBUG] SCHEMA WILL BE USED =====")
print("=========================================")
inferred_schema.simpleString()

df_parsed = df_sql.withColumn("parsed_values", from_json(col("values"), inferred_schema)) \
            .select("oid", "ts_date", "ts", "parsed_values.*") 
df_flattened = df_parsed \
            .withColumn("user_phone_number", col("phone_number").cast("long").cast(StringType()))\
            .withColumn("created_at_ts", (col("createdAt.`$date`") / 1000).cast(TimestampType())) \
            .withColumn("updated_at_ts", (col("updatedAt.`$date`") / 1000).cast(TimestampType())) \
            .withColumn("release_date", col("metadata.receivedAt").cast(TimestampType())) \
            .withColumn("message_body", col("metadata.metadata.entry").getItem(0).changes.getItem(0).value.messages.getItem(0).referral.body) \
            .withColumn("meta_ads_id", col("metadata.metadata.entry").getItem(0).changes.getItem(0).value.messages.getItem(0).referral.source_id) \
            .drop("__v", "phone_number", "_id", "createdAt", "updatedAt") 
df_flattened.show(10)
df_flattened.printSchema()

#-------------------------- Write Data To Hudi Table ------------------------------  
hudi_options = {
    'hoodie.table.name': HUDI_TABLE_NAME,
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.recordkey.field': 'oid', 
    'hoodie.datasource.write.operation': 'upsert', 
    'hoodie.datasource.write.partitionpath.field': 'ts_date',
    'hoodie.datasource.write.precombine.field': 'ts', 
    'hoodie.upsert.shuffle.parallelism': 2 ,
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'default',
    'hoodie.datasource.hive_sync.table': 'flat_kuponku_release',
    'hoodie.datasource.hive_sync.partition_fields': 'ts_date',
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true'
}

print("=========================================")
print("===== [DEBUG] WRITING TO HUDI TABLE =====")  
print("=========================================")
df_flattened.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(HUDI_PATH)

spark.stop()

