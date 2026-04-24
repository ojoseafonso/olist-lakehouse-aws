from utils.spark_session import get_spark
from utils import config
from pyspark.sql.functions import col, when, count
from pyspark.sql.types import StructField, StringType, TimestampType
from pyspark.sql.functions import to_timestamp


spark = get_spark("silver_orders")
# COMMAND ----------

# Lê o arquivo direto do catalog
df_orders = spark.read.format("parquet").load(f"s3a://{config.BUCKET_BRONZE}/olist_orders_dataset") \
.withColumn("order_purchase_timestamp",to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) \
.withColumn("order_approved_at",to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")) \
.withColumn("order_delivered_carrier_date",to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss")) \
.withColumn("order_delivered_customer_date",to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")) \
.withColumn("order_estimated_delivery_date",to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss")) \

df_orders = df_orders.cache()
df_orders.count()

# COMMAND ----------

# Verifica se nulos existem nas colunas que não podem ter nulos

colunas_obrigatorias = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]


# Conta nulos apenas nas colunas especificas
nulos = df_orders.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_obrigatorias)
])

nulos_dict = nulos.first().asDict()

for c in colunas_obrigatorias:
    if nulos_dict[c] > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.orders.{c}: {nulos_dict[c]} registros.")


# COMMAND ----------

# Lista de colunas para verificar campos que podem ter nulos
colunas_opcionais = ["order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]

# Conta nulos apenas nas colunas especificas
nulos = df_orders.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_opcionais
])

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_orders.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1)

if df_duplicados.limit(1).count() > 0:
    raise ValueError("[ERRO] PK duplicada em silver.orders.")

# COMMAND ----------

# Salva o df como delta
df_orders.write.format("parquet").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"s3a://{config.BUCKET_SILVER}/orders/")
