import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df = spark.read.parquet(args["input_path"]).select(
    "process_date", 
    "sector", 
    "ticker", 
    "quantity",
    "weight"
).cache()

#setor
window_date = Window.partitionBy("process_date")
df_sector = df.groupBy("process_date", "sector") \
    .agg(f.sum("quantity").alias("quantity")) \
    .withColumn("market_share", f.col("quantity") / f.sum("quantity").over(window_date))

#cod
window_ma = Window.partitionBy("ticker").orderBy("process_date").rowsBetween(-4, 0)
df_ticker = df.withColumn(
    "weight_moving_average", 
    f.avg("weight").over(window_ma)
).select(
    f.col("ticker"), 
    f.col("weight"), 
    f.col("weight_moving_average"), 
    f.col("process_date")
)

df_sector.coalesce(1).write.mode("overwrite").partitionBy("process_date").parquet(f'{args["output_path"]}/sector_market_share')
df_ticker.coalesce(1).write.mode("overwrite").partitionBy("process_date", "ticker").parquet(f'{args["output_path"]}/asset_moving_average')

job.commit()