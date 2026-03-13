import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType


args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path", "process_date"])

sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#schema
schema = StructType([
    StructField("sector", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("asset", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("cumulative_weight", StringType(), True),
])

#leitura
df = spark.read.option("sep", ";") \
    .option("header", "False") \
    .option("encoding", "latin1") \
    .schema(schema) \
    .csv(args["input_path"])

#tipagem
df = df.withColumn("quantity",  f.regexp_replace("quantity",  "\\.", "").cast("long")) \
    .withColumn("weight", f.regexp_replace("weight", ",",   ".").cast("float")) \
    .withColumn("cumulative_weight", f.regexp_replace("cumulative_weight", ",",   ".").cast("float")) \
    .withColumn("process_date", f.lit(args["process_date"]))

#tratamento de nulos
df = df.filter(~df["asset"].contains("Ação")).filter(~f.isnull(df["asset"]))

df.coalesce(1).write.mode("overwrite").partitionBy("process_date", "ticker").parquet(args["output_path"])

job.commit()
