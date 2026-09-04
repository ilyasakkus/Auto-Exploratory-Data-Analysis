"""Synthetic big dataset generator for testing SparkAutoEDA."""

import os
from datetime import datetime, timedelta
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
    TimestampType,
)


def generate_synthetic_transactions(
    spark: SparkSession,
    num_rows: int = 100_000,
    output_path: str = "sample_transactions.parquet",
):
    """Generate a realistic e-commerce transactions dataset with synthetic anomalies."""
    print(f"📦 Generating {num_rows:,} synthetic transactions on PySpark...")

    # We use RDD parallelize in chunks to scale efficiently
    num_partitions = max(4, os.cpu_count() or 4)
    rows_per_partition = num_rows // num_partitions

    base_time = datetime(2026, 1, 1, 0, 0, 0)
    payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Wire Transfer", "Crypto", None]
    devices = ["iOS", "Android", "Web Desktop", "Mobile Web", ""]
    segments = ["Consumer", "Corporate", "Small Business", "VIP"]
    countries = ["US", "DE", "GB", "TR", "FR", "JP", "CA", "AU"]

    def partition_generator(split_idx):
        random.seed(split_idx * 42 + 7)
        for i in range(rows_per_partition):
            # Normal order amount with occasional extreme outliers
            if random.random() < 0.02:
                amount = round(random.uniform(5000.0, 25000.0), 2)  # outlier
            else:
                amount = round(random.gauss(120.0, 45.0), 2)
                amount = max(5.0, amount)

            # Collinear tax_amount (~18% of amount + slight noise)
            tax = round(amount * 0.18 + random.uniform(-0.5, 0.5), 2)

            # Synthetic nulls for discount
            discount = None if random.random() < 0.25 else round(random.uniform(0.0, 0.35), 2)

            loyalty = int(amount * 1.5) if random.random() > 0.1 else 0
            order_time = base_time + timedelta(seconds=random.randint(0, 86400 * 90))
            pm = random.choice(payment_methods)
            dev = random.choice(devices)
            seg = random.choice(segments)
            country = random.choice(countries)
            is_fraud = True if (amount > 4000 or random.random() < 0.01) else False
            is_first = True if random.random() < 0.3 else False

            yield (
                int(split_idx * rows_per_partition + i + 1),  # transaction_id
                random.randint(1000, 50000),  # user_id
                float(amount),
                float(tax),
                discount,
                loyalty,
                pm,
                dev,
                seg,
                country,
                is_fraud,
                is_first,
                order_time,
            )

    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("user_id", LongType(), False),
        StructField("amount", DoubleType(), False),
        StructField("tax_amount", DoubleType(), False),
        StructField("discount_rate", DoubleType(), True),
        StructField("loyalty_points", LongType(), True),
        StructField("payment_method", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("is_fraudulent", BooleanType(), True),
        StructField("is_first_order", BooleanType(), True),
        StructField("order_timestamp", TimestampType(), True),
    ])

    rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions).flatMap(
        partition_generator
    )
    df = spark.createDataFrame(rdd, schema)

    # Add a few exact duplicate rows to test duplicate detection
    dup_sample = df.limit(int(num_rows * 0.02))
    df_with_dups = df.union(dup_sample)

    print(f"💾 Saving to Parquet: {output_path}...")
    df_with_dups.write.mode("overwrite").parquet(output_path)
    print(f"✨ Successfully generated {df_with_dups.count():,} rows at '{output_path}'.")
    return df_with_dups


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("SampleDataGenerator")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    generate_synthetic_transactions(spark, num_rows=100_000, output_path="sample_transactions.parquet")
    spark.stop()
