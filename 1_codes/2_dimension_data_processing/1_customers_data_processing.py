# **Import Required Libraries**

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# **Load Project Utilities & Initialize Notebook Widgets**

%run /Workspace/consolidated_pipeline/1_setup/utilities

print(bronze_schema, silver_schema, gold_schema)

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-final/{data_source}/*.csv'
print(base_path)

# ## Bronze

df = (
      spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", True)
          .load(base_path)
          .withColumn("read_timestamp", F.current_timestamp())
          .select("*", "_metadata.file_name", "_metadata.file_size")
)

df.printSchema()

display(df.limit(10))

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# ## Silver

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

df_bronze.printSchema()

# **Transformations**

# - 1: Drop Duplicates

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# - 2: Trim spaces in customer name

display(
      df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

df_silver = df_silver.withColumn(
      "customer_name",
      F.trim(F.col("customer_name"))
)

# - 3: Data Quality Fix: Correcting City Typos

df_silver.select('city').distinct().show()

# typos -> correct names
city_mapping = {
      'Bengaluruu': 'Bengaluru',
      'Bengalore': 'Bengaluru',

      'Hyderabadd': 'Hyderabad',
      'Hyderbad': 'Hyderabad',

      'NewDelhi': 'New Delhi',
      'NewDheli': 'New Delhi',
      'NewDelhee': 'New Delhi'
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
      df_silver
      .replace(city_mapping, subset=["city"])
      .withColumn(
                "city",
                F.when(F.col("city").isNull(), None)
                 .when(F.col("city").isin(allowed), F.col("city"))
                 .otherwise(None)
      )
)

df_silver.select('city').distinct().show()

# - 4: Fix Title-Casing Issue

df_silver.select('customer_name').distinct().show()

df_silver = df_silver.withColumn(
      "customer_name",
      F.when(F.col("customer_name").isNull(), None)
       .otherwise(F.initcap("customer_name"))
)

df_silver.select('customer_name').distinct().show()

# - 5: Handling missing cities

df_silver.filter(F.col("city").isNull()).show(truncate=False)

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
      789403: "New Delhi",   # Sprintx Nutrition
      789420: "Bengaluru",   # Zenathlete Foods
      789521: "Hyderabad",   # Primefuel Nutrition
      789603: "Hyderabad"    # Recovery Lane
}

df_fix = spark.createDataFrame(
      [(k, v) for k, v in customer_city_fix.items()],
      ["customer_id", "fixed_city"]
)

display(df_fix)

df_silver = (
      df_silver
      .join(df_fix, "customer_id", "left")
      .withColumn(
                "city",
                F.coalesce("city", "fixed_city")
      )
      .drop("fixed_city")
)

# - 6: Convert customer_id to string

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# ### Standardizing Customer Attributes to Match Parent Company Data Model

df_silver = (
      df_silver
      .withColumn(
                "customer",
                F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
      )
      .withColumn("market", F.lit("India"))
      .withColumn("platform", F.lit("Sports Bar"))
      .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(5))

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# ## Gold

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# ## Merging Data source with parent

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
      F.col("customer_id").alias("customer_code"),
      "customer",
      "market",
      "platform",
      "channel"
)

delta_table.alias("target").merge(
      source=df_child_customers.alias("source"),
      condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
