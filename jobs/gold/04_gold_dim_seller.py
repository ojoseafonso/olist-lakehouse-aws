from utils.spark_session import get_spark
from utils import config
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest, lag, lead, expr
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row
from pyspark.sql.window import Window

spark = get_spark("gold_dim_seller")
# COMMAND ----------

# Lê o arquivo direto do catalog
dim_seller = spark.read.load(f"s3a://{config.BUCKET_SILVER}/sellers/")

# COMMAND ----------

# Deduplicação pela chave natural
dim_seller = dim_seller.dropDuplicates(["seller_id"])

# COMMAND ----------

# Criação de SK Hash32
dim_seller = dim_seller.withColumn("sk_seller",sha2(concat(lit("dim_seller||"), col("seller_id").cast("string")), 256))

# COMMAND ----------

dim_seller = dim_seller.select(
    "sk_seller",
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
)

# COMMAND ----------

# Salva o df como delta
dim_seller.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"s3a://{config.BUCKET_GOLD}/dim_seller/")