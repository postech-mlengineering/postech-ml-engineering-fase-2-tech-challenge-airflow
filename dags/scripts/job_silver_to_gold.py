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

df = spark.read.parquet(args["input_path"])

window_spec = Window.partitionBy("cod").orderBy("process_date").rowsBetween(-2, 0)

df_media_movel_cod = df.withColumn(
    "media_movel_part", 
    f.avg("part").over(window_spec)
).select(
    f.col("cod"), 
    f.col("part"), 
    f.col("media_movel_part"), 
    f.col("process_date")
)

df_soma_qtd_setor = df.groupBy('process_date', 'setor').agg(f.sum(f.col('qtd')))

df_media_movel_cod.coalesce(1) .write.mode("overwrite").partitionBy("process_date", "cod").parquet(f'{args["output_path"]}/cod')
df_soma_qtd_setor.coalesce(1) .write.mode("overwrite").partitionBy("process_date").parquet(f'{args["output_path"]}/setor')

job.commit()