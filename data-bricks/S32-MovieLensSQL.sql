-- Databricks notebook source
SHOW TABLES IN moviedb

-- COMMAND ----------

SELECT movieid, avg(rating) as avg_rating, count(userId) as total_ratings 
 FROM moviedb.ratings_csv
 GROUP BY movieid
 HAVING avg_rating >= 3.5 AND total_ratings >= 100

-- COMMAND ----------

-- temp view/temp table
-- based on per spark session, available until spark notebook running

CREATE OR REPLACE TEMP VIEW popular_movies AS
SELECT movieid, avg(rating) as avg_rating, count(userId) as total_ratings 
 FROM moviedb.ratings_csv
 GROUP BY movieid
 HAVING avg_rating >= 3.5 AND total_ratings >= 100

-- COMMAND ----------

SHOW TABLES
-- temp views must be created in default db, limited to spark session, not accessible in another notebook

-- COMMAND ----------

SELECT * FROM popular_movies LIMIT 10

-- COMMAND ----------

-- create permanent table using CTAS
CREATE TABLE moviedb.most_popular_movies AS
SELECT movies.movieid, title, avg_rating, total_ratings 
FROM popular_movies 
INNER JOIN moviedb.movies_csv movies 
ON popular_movies.movieid = movies.movieid

-- COMMAND ----------

SHOW TABLES IN moviedb

-- COMMAND ----------

SELECT * FROM moviedb.most_popular_movies

-- COMMAND ----------

EXPLAIN EXTENDED SELECT movies.movieid, title, avg_rating, total_ratings 
FROM popular_movies 
INNER JOIN moviedb.movies_csv movies 
ON popular_movies.movieid = movies.movieid

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC SELECT movies.movieid, title, avg_rating, total_ratings 
-- MAGIC FROM popular_movies 
-- MAGIC INNER JOIN moviedb.movies_csv movies 
-- MAGIC ON popular_movies.movieid = movies.movieid
-- MAGIC """).explain(extended=True)

-- COMMAND ----------

