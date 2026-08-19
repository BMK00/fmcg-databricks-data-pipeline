# **Import Required Libraries**

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# **Load Project Utilities & Initialize Notebook Widgets**

%run /Workspace/consolidated_pipeline/1_setup/utilities

print(bronze_schema, silver_schema, gold_schema)

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

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

# **Transformations**

# - 1: Drop Duplicates

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# - 2: Title case fix
# (energy bars ---> Energy Bars, protien bars ---> Protien Bars etc)

df_silver.select('category').distinct().show()

df_silver = df_silver.withColumn(
      "category",
      F.when(F.col("category").isNull(), None)
       .otherwise(F.initcap("category"))
)

df_silver.select('category').distinct().show()

# - 3: Fix Spelling Mistake for `Protien`

df_silver = (
      df_silver
      .withColumn(
                "product_name",
                F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
      )
      .withColumn(
                "category",
                F.regexp_replace(F.col("category"), "(?i)Protien", "Protein")
      )
)

display(df_silver.limit(5))

# ### Standardizing Customer Attributes to Match Parent Company Data Model

df_silver = (
      df_silver
      .withColumn(
                "division",
                F.when(F.col("category") == "Energy Bars",        "Nutrition Bars")
                 .when(F.col("category") == "Protein Bars",       "Nutrition Bars")
                 .when(F.col("category") == "Granola & Cereals",  "Breakfast Foods")
                 .when(F.col("category") == "Recovery Dairy",     "Dairy & Recovery")
                 .when(F.col("category") == "Healthy Snacks",     "Healthy Snacks")
                 .when(F.col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
                 .otherwise("Other")
      )
)

df_silver = df_silver.withColumn(
      "variant",
      F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)

# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver = (
      df_silver
      .withColumn(
                "product_code",
                F.sha2(F.col("product_name").cast("string"), 256)
      )
      .withColumn(
                "product_id",
                F.when(
                              F.col("product_id").cast("string").rlike("^[0-9]+$"),
                              F.col("product_id").cast("string")
                ).otherwise(F.lit(999999).cast("string"))
      )
      .withColumnRenamed("product_name", "product")
)

df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

display(df_silver)

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# ## Gold

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
df_gold.show(5)

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# ## Merging Data source with parent

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

delta_table.alias("target").merge(
      source=df_child_products.alias("source"),
      condition="target.product_code = source.product_code"
).whenMatchedUpdate(
      set={
                "division": "source.division",
                "category": "source.category",
                "product": "source.product",
                "variant": "source.variant"
      }
).whenNotMatchedInsert(
      values={
                "product_code": "source.product_code",
                "division": "source.division",
                "category": "source.category",
                "product": "source.product",
                "variant": "source.variant"
      }
).execute()
