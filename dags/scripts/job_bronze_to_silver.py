import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from datetime import datetime

# 1. Obter argumentos passados pelo Airflow GlueJobOperator
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

# 2. Inicializar contexto do Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 3. Leitura do arquivo CSV da zona Bronze
# Encoding latin1 é padrão para arquivos da B3

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
      .option("header", "true")
      .option("encoding", "latin1")
      .option("skipRows", 2) # pula linha 1 (titutlo) pegar o header na segunda
      .schema(schema)
      .csv(args['input_path'])
      )

# Limpeza — usa o nome correto da coluna
df = df.filter(F.col("cod").isNotNull() & (F.col("cod") != ""))

# Transformações
df = df \
    .withColumn("qtd",  F.regexp_replace("qtd",  "\\.", "").cast("long")) \
    .withColumn("part", F.regexp_replace("part", ",",   ".").cast("float")) \
    .withColumn("acum", F.regexp_replace("acum", ",",   ".").cast("float")) \
    .withColumn("data_pregao", F.lit(datetime.now().strftime("%Y-%m-%d")))

df.write.mode("overwrite").parquet(args['output_path'])

job.commit()