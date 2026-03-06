import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType


args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path", "process_date"])

#nicializa contexto do spark/glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#leitura do arquivo camada bronze
#encoding latin1 é padrão para arquivos da B3
schema = StructType([
    StructField("setor", StringType(), True),
    StructField("cod",   StringType(), True),
    StructField("acao",  StringType(), True),
    StructField("tipo",  StringType(), True),
    StructField("qtd",   StringType(), True),
    StructField("part",  StringType(), True),
    StructField("acum",  StringType(), True),
])

df = (spark.read
      .option("sep", ";")
      .option("header", "False")
      .option("encoding", "latin1")
      .schema(schema)
      .csv(args["input_path"])
      )

#transformações
df = df \
    .withColumn("qtd",  f.regexp_replace("qtd",  "\\.", "").cast("long")) \
    .withColumn("part", f.regexp_replace("part", ",",   ".").cast("float")) \
    .withColumn("acum", f.regexp_replace("acum", ",",   ".").cast("float")) \
    .withColumn("process_date", f.lit(args["process_date"]))

#limpeza
df = df.filter(~df["acao"].contains("Ação")) \
       .filter(~f.isnull(df["acao"]))

df.write.mode("overwrite").partitionBy("process_date", "cod").parquet(args["output_path"])
job.commit()