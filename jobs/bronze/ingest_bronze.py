from utils.spark_session import get_spark
from utils import config
 
spark = get_spark("bronze_ingest")


path = {"input": "/opt/airflow/raw_data",
		"output": f"s3a://{config.BUCKET_BRONZE}"
}

files = ["olist_orders_dataset",
"olist_products_dataset",
"olist_order_items_dataset",
"olist_order_reviews_dataset",
"olist_customers_dataset",
"olist_sellers_dataset",
"olist_order_payments_dataset",
"olist_geolocation_dataset",
"product_category_name_translation"]

for f in files:
    df = spark.read.csv(
        f"{path['input']}/{f}.csv",
        header=True,
        encoding="utf-8",
    )
	df.write.mode("overwrite").parquet(f"{path["output"]}/{f}/")

spark.stop()