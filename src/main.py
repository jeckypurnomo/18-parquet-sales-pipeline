from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, col, sum

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("PySparkParquetDataPipeline")
    .master("local[*]")
    .getOrCreate()
)


# ==== EXTRACT ====


# Read the Dataset
orders_df = (
    spark.read.csv(
        "data/orders.csv",
        header=True,
        inferSchema=True
    )
)

# Display the Dataset
print("\n--- Orders Dataset ---")
orders_df.show()

# Display Dataset Schema
print("\n--- Orders Dataset Schema ---")
orders_df.printSchema()


# ==== TRANSFORM ====


# Convert date type 
orders_date_df = (
    orders_df
    .withColumn(
        "order_date",
        to_date("order_date")
    )
)

# Display Converted Orders Date Schema
print("\n--- Orders Schema after Date Converted ---")
orders_date_df.printSchema()

# Create year and month on Dataset
orders_date_diverge_df = (
    orders_date_df
    .withColumn(
        "year",
        year("order_date")
    )
    .withColumn(
        "month",
        month("order_date")
    )
)

# Display Dataset After Create Year and Month
print("\n--- Orders Dataset with Year and Month ---")
orders_date_diverge_df.show()

# Calculate Total Amount
orders_total_amount_df = (
    orders_date_diverge_df
    .withColumn(
        "total_amount",
        col("quantity") * col("unit_price")
    )
)

# Display the Calculated Dataset
print("\n--- Orders Dataset with Total Amount ---")
orders_total_amount_df.show()

# Filter the Dataset
clean_orders_df = (
    orders_total_amount_df
    .filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("order_date").isNotNull())
    )
)

# Display the Clean Dataset
print("\n--- Clean Orders Dataset ---")
clean_orders_df.show()


# ==== LOAD ====


# Load the Clean Dataset
clean_orders_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("output/parquet_orders/")

print("\nParquet Orders Dataset Saved Successfully.")


# ==== ADDITIONAL ====


# Read the Parquet Dataset
parquet_orders_df = spark.read.parquet(
    "output/parquet_orders/"
)

# Display the Dataset
print("\n--- Parquet Orders Dataset ---")
parquet_orders_df.show()

# Display Dataset Schema
print("\n--- Parquet Orders Dataset Schema ---")
parquet_orders_df.printSchema()

# Read Specific Dataset
specific_parquet_orders_df = (
    parquet_orders_df
    .filter(
        (col("year") == 2026) &
        (col("month") == 3)
    )
)

# Display the Specific Dataset
print("\n--- March 2026 Dataset ---")
specific_parquet_orders_df.show()

# Calculate the Revenue
specific_orders_revenue_df = (
    specific_parquet_orders_df
    .groupBy(
        "year",
        "month"
    )
    .agg(
        sum("total_amount").alias("total_revenue")
    )
)

# Display Calculated Revenue
print("\n--- March 2026 Revenue ---")
specific_orders_revenue_df.show()

total_records_count = orders_df.count()
record_2025_count = (
    clean_orders_df
    .filter(
        col("year") == 2025
    )
    .count()
)
record_2026_count = (
    clean_orders_df
    .filter(
        col("year") == 2026
    )
    .count()
)
march_2026_orders_count = specific_parquet_orders_df.count()
result = specific_orders_revenue_df.first()
march_2026_revenue = result["total_revenue"]

print("\n" + "=" * 40)
print("Parquet Sales Pipeline Completed")
print("=" * 40)
print(f"Total Records : {total_records_count}")
print(f"2025 Records : {record_2025_count}")
print(f"2026 Records : {record_2026_count}")
print(f"March 2026 Orders : {march_2026_orders_count}")
print(f"March 2026 Revenue : {march_2026_revenue}")

spark.stop()