# Databricks notebook source
# MAGIC %md
# MAGIC # 🍕 Pizza Sales — Medallion Pipeline (Databricks)
# MAGIC **Sources:** Database (menu tables) + Object Store (order files in a Volume)
# MAGIC **Objectives:** ① Best-selling pizzas  ② Revenue by day of week
# MAGIC **Flow:** Bronze (raw) → Silver (clean + join) → Gold (aggregated) → Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config — names you can change

# COMMAND ----------

CATALOG = "workspace"          # default catalog on Databricks Free edition; change if yours differs
SCHEMA  = "pizza_project"
VOLUME  = "raw_files"

VOL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print("Volume path:", VOL_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Upload your files (do this once, in the UI)
# MAGIC Go to **Catalog → workspace → pizza_project → raw_files** and create these folders, then upload:
# MAGIC - `menu/`  → pizzas.csv, pizza_types.csv   *(this is our DATABASE source)*
# MAGIC - `orders/` → orders.csv   *(OBJECT STORE source — the whole folder is read)*
# MAGIC - `order_details/` → order_details.csv   *(OBJECT STORE source)*
# MAGIC
# MAGIC Run the cell below to confirm the files are there.

# COMMAND ----------

display(dbutils.fs.ls(f"{VOL_PATH}/orders"))
display(dbutils.fs.ls(f"{VOL_PATH}/order_details"))
display(dbutils.fs.ls(f"{VOL_PATH}/menu"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🥉 BRONZE — read raw data exactly as-is
# MAGIC We read whole folders. That's the trick: drop a NEW file in `orders/` later and it gets picked up automatically.

# COMMAND ----------

# --- DATABASE source: the menu ---
bronze_pizzas = spark.read.option("header", True).option("inferSchema", True).csv(f"{VOL_PATH}/menu/pizzas.csv")
bronze_pizza_types = spark.read.option("header", True).option("inferSchema", True).csv(f"{VOL_PATH}/menu/pizza_types.csv")

# --- OBJECT STORE source: the orders (read the FOLDER, not one file) ---
bronze_orders = spark.read.option("header", True).option("inferSchema", True).csv(f"{VOL_PATH}/orders/")
bronze_order_details = spark.read.option("header", True).option("inferSchema", True).csv(f"{VOL_PATH}/order_details/")

# save raw, untouched, as Delta (the bronze layer)
bronze_pizzas.write.mode("overwrite").saveAsTable("bronze_pizzas")
bronze_pizza_types.write.mode("overwrite").saveAsTable("bronze_pizza_types")
bronze_orders.write.mode("overwrite").saveAsTable("bronze_orders")
bronze_order_details.write.mode("overwrite").saveAsTable("bronze_order_details")

print("Bronze row counts:")
print("  orders        :", bronze_orders.count())
print("  order_details :", bronze_order_details.count())
print("  pizzas        :", bronze_pizzas.count())
print("  pizza_types   :", bronze_pizza_types.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🥈 SILVER — clean + join into one tidy "sales" table

# COMMAND ----------

from pyspark.sql import functions as F

orders        = spark.table("bronze_orders").withColumn("order_date", F.to_date("date"))
order_details = spark.table("bronze_order_details")
pizzas        = spark.table("bronze_pizzas")
pizza_types   = spark.table("bronze_pizza_types")

silver_sales = (
    order_details
        .join(orders,      "order_id")
        .join(pizzas,      "pizza_id")
        .join(pizza_types, "pizza_type_id")
        .withColumn("line_revenue", F.col("quantity") * F.col("price"))
        .withColumn("day_of_week",  F.date_format("order_date", "E"))   # Mon, Tue...
        .withColumn("dow_num",      F.dayofweek("order_date"))           # 1=Sun ... for sorting
        .select(
            "order_id", "order_date", "day_of_week", "dow_num",
            "pizza_id", F.col("name").alias("pizza_name"), "category", "size",
            "quantity", F.col("price").alias("unit_price"), "line_revenue",
        )
        .dropDuplicates()
        .filter(F.col("quantity") > 0)        # basic cleaning
)

silver_sales.write.mode("overwrite").saveAsTable("silver_sales")
print("Silver rows:", silver_sales.count())
display(silver_sales.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 🥇 GOLD — the two business objectives

# COMMAND ----------

s = spark.table("silver_sales")

# Objective ① Best-selling pizzas
gold_best_sellers = (
    s.groupBy("pizza_name")
     .agg(F.sum("quantity").alias("total_quantity"),
          F.round(F.sum("line_revenue"), 2).alias("total_revenue"))
     .orderBy(F.desc("total_quantity"))
)
gold_best_sellers.write.mode("overwrite").saveAsTable("gold_best_sellers")

# Objective ② Revenue by day of week
gold_revenue_by_day = (
    s.groupBy("day_of_week", "dow_num")
     .agg(F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
          F.countDistinct("order_id").alias("order_count"))
     .orderBy("dow_num")
)
gold_revenue_by_day.write.mode("overwrite").saveAsTable("gold_revenue_by_day")

print("=== Objective ① Top pizzas ===")
display(gold_best_sellers.limit(10))
print("=== Objective ② Revenue by day ===")
display(gold_revenue_by_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 📊 Dashboard queries
# MAGIC Run each query, then click the **+ Visualization** button under the result to make a chart.
# MAGIC Add both charts to a dashboard (see the chat steps).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Objective ①: best-selling pizzas (bar chart: pizza_name vs total_quantity)
# MAGIC SELECT pizza_name, total_quantity, total_revenue
# MAGIC FROM gold_best_sellers
# MAGIC ORDER BY total_quantity DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Objective ②: revenue by day of week (bar chart: day_of_week vs total_revenue)
# MAGIC SELECT day_of_week, total_revenue, order_count
# MAGIC FROM gold_revenue_by_day
# MAGIC ORDER BY dow_num;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ✅ The auto-update test (this is what the professor checks)
# MAGIC 1. Upload `new_orders.csv` into the **orders/** folder and `new_order_details.csv` into **order_details/**.
# MAGIC 2. Run **Bronze → Silver → Gold** again (or "Run all").
# MAGIC 3. Re-run the dashboard queries → the numbers move on their own, because Bronze reads the *whole folder*.
# MAGIC
# MAGIC No code changes. That's the proof your pipeline is automated.
