import os

BUCKET_BRONZE = os.environ.get("BUCKET_BRONZE", "olist-lakehouse-bronze")
BUCKET_SILVER = os.environ.get("BUCKET_SILVER", "olist-lakehouse-silver")
BUCKET_GOLD   = os.environ.get("BUCKET_GOLD",   "olist-lakehouse-gold")