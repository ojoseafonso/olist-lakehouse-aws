from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

SPARK_IMAGE = "olist-spark:latest"
DOCKER_URL = "unix://var/run/docker.sock"
COMMON = dict(
    image=SPARK_IMAGE,
    docker_url=DOCKER_URL,
    auto_remove=True,
    network_mode="bridge",
)

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_tasks=2,
    tags=["olist", "lakehouse"],
) as dag:

    # ── BRONZE ───────────────────────────────────────────────
    bronze_ingest = DockerOperator(
        task_id="bronze_ingest",
        command="spark-submit /jobs/bronze/ingest_bronze.py",
        **COMMON,
    )

    # ── SILVER ───────────────────────────────────────────────
    silver_orders = DockerOperator(
        task_id="silver_orders",
        command="spark-submit /jobs/silver/01_silver_orders.py",
        **COMMON,
    )
    silver_products = DockerOperator(
        task_id="silver_products",
        command="spark-submit /jobs/silver/02_silver_products.py",
        **COMMON,
    )
    silver_order_items = DockerOperator(
        task_id="silver_order_items",
        command="spark-submit /jobs/silver/03_silver_order_items.py",
        **COMMON,
    )
    silver_reviews = DockerOperator(
        task_id="silver_reviews",
        command="spark-submit /jobs/silver/04_silver_reviews.py",
        **COMMON,
    )
    silver_customers = DockerOperator(
        task_id="silver_customers",
        command="spark-submit /jobs/silver/05_silver_customers.py",
        **COMMON,
    )
    silver_sellers = DockerOperator(
        task_id="silver_sellers",
        command="spark-submit /jobs/silver/06_silver_sellers.py",
        **COMMON,
    )
    silver_payments = DockerOperator(
        task_id="silver_payments",
        command="spark-submit /jobs/silver/07_silver_payments.py",
        **COMMON,
    )
    silver_geolocation = DockerOperator(
        task_id="silver_geolocation",
        command="spark-submit /jobs/silver/08_silver_geolocation.py",
        **COMMON,
    )
    silver_category = DockerOperator(
        task_id="silver_category",
        command="spark-submit /jobs/silver/09_silver_product_category_name_translation.py",
        **COMMON,
    )

    # ── GOLD DIMENSÕES ───────────────────────────────────────
    gold_dim_tempo = DockerOperator(
        task_id="gold_dim_tempo",
        command="spark-submit /jobs/gold/01_gold_dim_tempo.py",
        **COMMON,
    )
    gold_dim_cliente = DockerOperator(
        task_id="gold_dim_cliente",
        command="spark-submit /jobs/gold/02_gold_dim_cliente.py",
        **COMMON,
    )
    gold_dim_produto = DockerOperator(
        task_id="gold_dim_produto",
        command="spark-submit /jobs/gold/03_gold_dim_produto.py",
        **COMMON,
    )
    gold_dim_seller = DockerOperator(
        task_id="gold_dim_seller",
        command="spark-submit /jobs/gold/04_gold_dim_seller.py",
        **COMMON,
    )

    # ── GOLD FATOS ───────────────────────────────────────────
    gold_fato_itens = DockerOperator(
        task_id="gold_fato_itens_pedido",
        command="spark-submit /jobs/gold/05_gold_fato_itens_pedido.py",
        **COMMON,
    )
    gold_fato_pagamentos = DockerOperator(
        task_id="gold_fato_pagamentos",
        command="spark-submit /jobs/gold/06_gold_fato_pagamentos.py",
        **COMMON,
    )
    gold_fato_avaliacoes = DockerOperator(
        task_id="gold_fato_avaliacoes",
        command="spark-submit /jobs/gold/07_gold_fato_avaliacoes.py",
        **COMMON,
    )

    # ── DEPENDÊNCIAS ─────────────────────────────────────────
    silver_all = [
        silver_orders, silver_products, silver_order_items,
        silver_reviews, silver_customers, silver_sellers,
        silver_payments, silver_geolocation, silver_category,
    ]

    bronze_ingest >> silver_all

    silver_orders >> gold_dim_tempo
    [silver_orders, silver_customers] >> gold_dim_cliente
    silver_products >> gold_dim_produto
    silver_sellers >> gold_dim_seller

    gold_dims = [gold_dim_tempo, gold_dim_cliente, gold_dim_produto, gold_dim_seller]

    gold_dims >> gold_fato_itens
    [gold_dim_tempo, gold_dim_cliente] >> gold_fato_pagamentos
    [gold_dim_tempo, gold_dim_cliente] >> gold_fato_avaliacoes
