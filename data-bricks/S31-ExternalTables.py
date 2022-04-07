# Databricks notebook source
# external table, schemas are with hive, data is managed by eTL or external operations

df = spark.sql ("SELECT * FROM moviedb.movies_csv")
df.printSchema()
df.show(5)

# COMMAND ----------

# get table into dataframe
df = spark.table("moviedb.movies_csv")
df.printSchema()
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.movies_csv LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED moviedb.reviews 
# MAGIC -- managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED moviedb.movies_csv 
# MAGIC -- external/unmanaged table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- print CREATE TABLE from exisitng table 
# MAGIC 
# MAGIC SHOW CREATE TABLE moviedb.movies_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE moviedb.ratings_csv ( userId INT, movieId INT, rating DOUBLE, timestamp LONG ) 
# MAGIC USING csv OPTIONS ( 'delimiter' = ',', 
# MAGIC                     'escape' = '"', 
# MAGIC                     'header' = 'true',
# MAGIC                      'multiLine' = 'false')
# MAGIC           LOCATION 'dbfs:/FileStore/tables/ml-latest-small/ratings.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN moviedb

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM moviedb.ratings_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED moviedb.ratings_csv

# COMMAND ----------

