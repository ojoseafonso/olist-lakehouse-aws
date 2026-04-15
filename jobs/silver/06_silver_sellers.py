from utils.spark_session import get_spark
from utils import config
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

spark = get_spark("silver_sellers")
# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), False),
    StructField("seller_city", StringType(), False),
    StructField("seller_state", StringType(), False)
])

# Lê o arquivo direto do catalog
df_sellers = spark.read.format("parquet").schema(schema).option("header", "true").load(f"s3a://{config.BUCKET_BRONZE}/olist_sellers_dataset")

# COMMAND ----------

# Verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]

# Conta nulos apenas nas colunas especificas
nulos = df_sellers.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.sellers.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_sellers.count()
distinct_rows = df_sellers.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.sellers.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_sellers.groupBy("seller_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.sellers: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.sellers"
df_sellers.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"s3a://{config.BUCKET_SILVER}/sellers/")