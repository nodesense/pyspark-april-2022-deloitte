# Databricks notebook source
#/FileStore/tables/stocks/delivery/sec_bhavdata_full_04042022.csv


# COMMAND ----------

deliveryDf = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/FileStore/tables/stocks/delivery")

deliveryDf.printSchema()
deliveryDf.show(2)

# COMMAND ----------

from pyspark.sql import functions as F

deliveryDf = deliveryDf.select([F.col(col).alias(col.replace(' ', '')) for col in deliveryDf.columns])
deliveryDf.printSchema()


# COMMAND ----------

# GAP UP - Diff between Today's Opening Price - Yesterdays Closing Price
# Derive a column named "GAP", that subtract OPEN_PRICE - PREV_CLOSE
# derive a column "GAP_STATUS" that state gap up 'U' or gap down 'D' or same/equal 'E' using when
import pyspark.sql.functions as F
deliveryDfGap = deliveryDf.withColumn("GAP", deliveryDf.OPEN_PRICE - deliveryDf.PREV_CLOSE)\
                          .withColumn("GAP_STATUS", F.when(F.col("GAP") > 0, 'U')
                                                     .when(F.col("GAP") < 0, 'D')
                                                     .otherwise('E')
                                     )
deliveryDfGap.printSchema()
deliveryDfGap.show(2)

# COMMAND ----------

# get count of gap up, gap down, no change in stocks

df = deliveryDfGap.groupBy("GAP_STATUS")\
             .agg(F.count(deliveryDfGap.GAP_STATUS))

df.show()

# COMMAND ----------

# get few stocks sorted by decending order based on gap, gap up

df = (
  deliveryDfGap.filter ( F.col("GAP_STATUS") == 'U')
               .sort(F.col("GAP").desc())
               .select("SYMBOL", "SERIES", "PREV_CLOSE", "OPEN_PRICE", "GAP")
)

df.show()
   

# COMMAND ----------

