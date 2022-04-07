# Databricks notebook source
# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), #  no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
   (400, "Alphabet"),
    (500, "Sony"), # no matching products
]
 
productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
productDf.show()
brandDf.show()

# COMMAND ----------

spark # spark session

# COMMAND ----------

# create temp table using spark session/dataframe
# productDf created from spark session spark.createDataFrame...
productDf.createOrReplaceTempView("products") # created inside spark session

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

spark.sql("SELECT * FROM PRODUCTS").show()

# COMMAND ----------

# create a new session
spark2 = spark.newSession()

# COMMAND ----------

# now access, products table from spark2 session will cause error
# by default temp table/temp views are limited with in session
# AnalysisException: Table or view not found: PRODUCTS; 
spark2.sql("SELECT * FROM PRODUCTS").show()

# COMMAND ----------

stock_symbols = [ 
          # (product_name, sector)  
         ('INFY', "IT"),
         ('TCS', "IT"),
         ('TATAMOTORS', "AUTO")
]
 
productSymbolDf = spark2.createDataFrame(data=stock_symbols, schema=[  "product_name", "sector"])

productSymbolDf.show()
# product table created under session 2 spark2
productSymbolDf.createOrReplaceTempView("products")

# COMMAND ----------

spark2.sql("SELECT * FROM PRODUCTS").show()

# COMMAND ----------

spark.sql("SELECT * FROM PRODUCTS").show()

# COMMAND ----------

# global temp view, which allows the temp views can be shared across multiple 
# spark sessions
# limited until the notebook is running, global temp view, temp purpose
brandDf.createOrReplaceGlobalTempView("brands")

spark.sql("SHOW TABLES in global_temp").show()

# COMMAND ----------

# now query brands from global_temp using spark session
spark.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------

# now query brands from global_temp using spark2 session
spark2.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------

