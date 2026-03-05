import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime

# 1. Obter argumentos passados pelo Airflow GlueJobOperator
args = getResolvedOptions(sys.argv, ['JOB_NAME', '--input_path', '--output_path'])

# 2. Inicializar contexto do Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 3. Leitura do arquivo CSV da zona Bronze
# Encoding latin1 é padrão para arquivos da B3
df = spark.read \
    .option("sep", ";") \
    .option("header", "false") \
    .option("encoding", "latin1") \
    .csv(args['input_path'])

# 4. Limpeza inicial: Remover linhas vazias ou cabeçalhos indesejados
# Ajuste o filtro conforme a necessidade (ex: se o arquivo tem linhas extras no início)
df = df.filter(F.col("_c1").isNotNull()) 

# 5. Renomear colunas conforme seu padrão original
df = df.toDF("setor", "cod", "acao", "tipo", "qtd", "part", "acum")

# 6. Transformações de dados
# - Substituição de pontos por vazio em inteiros
# - Substituição de vírgulas por pontos em decimais
# - Adição da data do pregão
df = df.withColumn("qtd", F.regexp_replace("qtd", "\\.", "").cast("int")) \
       .withColumn("part", F.regexp_replace("part", ",", ".").cast("float")) \
       .withColumn("acum", F.regexp_replace("acum", ",", ".").cast("float")) \
       .withColumn("data_pregao", F.lit(datetime.now().strftime("%Y-%m-%d")))

# 7. Escrita na zona Silver (Formato Parquet)
# O modo overwrite garante que, se rodar novamente no mesmo dia, o arquivo seja atualizado
df.write.mode("overwrite").parquet(args['output_path'])

# Finalizar job
job.commit()