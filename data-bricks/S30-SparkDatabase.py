# Databricks notebook source
spark # spark session based on hive

# COMMAND ----------

# dbutils.fs.rm("/user/hive/warehouse", True)

# COMMAND ----------

# create database called moviedb
spark.sql ("CREATE DATABASE IF NOT EXISTS moviedb")

# COMMAND ----------

df = spark.sql("SHOW DATABASES")
df.printSchema()
df.show()

# COMMAND ----------

# TWO TYPES OF TABLES
# 1. Managed table - INSERT, UPDATE, DELETE to be performed by Spark Engine
#                    Data shall be stored in /user/hive/warehouse/database/table.db 
#                - Schema and data both managed by spark/hive meta data
# 2. Unmanaged table /external table
#        - schema is managed by spark/hive meta data
#        - data is managed by externally by people, or ETL tools add new files, remove exisiting file etc

# COMMAND ----------

# Create a managed table
spark.sql("""
CREATE TABLE IF NOT EXISTS moviedb.reviews (movieId INT, review STRING)
""")

# in data, check /user/hive/warehouse/moviedb.db/reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sql comment
# MAGIC -- magic function, we can write SQL directly
# MAGIC -- SQL shall be executed inside spark.sql when we run the cell
# MAGIC -- the output dataframe shall be displayed using a display function/html 5
# MAGIC 
# MAGIC INSERT INTO moviedb.reviews VALUES (1, 'nice movie')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO moviedb.reviews VALUES (2, 'funny')

# COMMAND ----------

df = spark.sql("SELECT * FROM moviedb.reviews")
df.show() # ASCII Table
display (df) # Databricsk html 5 output

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update, delete supported with delta lake
# MAGIC UPDATE moviedb.reviews set review='comedy' where movieid=2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete supported by delta
# MAGIC DELETE from moviedb.reviews where movieid=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

