from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SILVER_SCHEMA = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount_eur", DoubleType(), True),
    ]
)


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Lecture du Parquet Silver avec schéma strict."""
    silver_path = f"data/raw/dt={logical_date}"
    return spark.read.schema(SILVER_SCHEMA).parquet(silver_path)


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Enrichissement : filtre corrompus, ajoute revenue_class et date."""
    df = df.filter(F.col("amount_eur") > 0)
    df = df.withColumn(
        "revenue_class", F.when(F.col("amount_eur") > 100, "high").otherwise("low")
    )
    return df.withColumn("logical_date", F.lit(logical_date))


def transform_3(df: DataFrame) -> DataFrame:
    """Agrégation KPIs par catégorie et pays.

    Note shuffle : groupBy déclenche un shuffle sur la colonne category+country.
    Acceptable ici car le volume Silver est ~200 lignes/jour.
    En production sur des millions de lignes, on partitionnerait par category
    en amont pour éviter ce shuffle.
    """
    return df.groupBy("category", "country").agg(
        F.round(F.sum("amount_eur"), 2).alias("total_revenue"),
        F.count("tx_id").alias("transaction_count"),
    )


def transform_4(spark: SparkSession, df_kpi: DataFrame) -> DataFrame:
    """Broadcast join avec les objectifs de référence par catégorie.

    On utilise broadcast() car category_targets est petit (~10 lignes).
    Cela évite un shuffle coûteux entre les workers Spark.
    """
    ref_path = "data/reference/category_targets.csv"
    df_targets = (
        spark.read.option("header", True).option("inferSchema", True).csv(ref_path)
    )

    df_joined = df_kpi.join(broadcast(df_targets), on="category", how="left")

    return df_joined.withColumn(
        "target_reached",
        F.when(F.col("total_revenue") >= F.col("target_revenue_eur"), "yes").otherwise(
            "no"
        ),
    )


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Appelé par Airflow : enchaîne les 4 transforms et écrit les sorties."""
    spark = (
        SparkSession.builder.appName(f"team_marie_kpis_{logical_date}")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_silver = transform_1(spark, logical_date)
    df_enrichi = transform_2(spark, df_silver, logical_date)
    df_kpi = transform_3(df_enrichi)
    df_final = transform_4(spark, df_kpi)  # broadcast join

    # Écriture Gold — overwrite = idempotent
    curated_path = f"data/curated/dt={logical_date}"
    df_final.write.mode("overwrite").parquet(curated_path)

    # Rapport JSON
    totals = df_enrichi.agg(
        F.round(F.sum("amount_eur"), 2).alias("total_revenue"),
        F.count("tx_id").alias("total_transactions"),
    ).collect()[0]

    report = {
        "status": "ok",
        "logical_date": logical_date,
        "total_revenue": totals["total_revenue"],
        "total_transactions": totals["total_transactions"],
        "curated_path": curated_path,
        "generated_at": datetime.utcnow().isoformat(),
    }

    Path("data/reports").mkdir(parents=True, exist_ok=True)
    with open(f"data/reports/dashboard_{logical_date}.json", "w") as f:
        json.dump(report, f, indent=2)

    spark.stop()
    return report
