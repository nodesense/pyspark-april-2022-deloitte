# Databricks notebook source
# dynamically increase/decrese partitions

rdd = sc.parallelize(range(0, 500)) # 8 partitions

# COMMAND ----------

rdd.glom().collect()

# COMMAND ----------

# repartition useful to increase the partition 
# repartition also useful to shuffle data to balance to more number of partitions
rdd2 = rdd.repartition(10)

print (rdd2.glom().collect())
print("partitions ", rdd2.getNumPartitions())


# COMMAND ----------

# coalesce  to reduce number of partitions
# repartition - shuffle data by default across machine
# coalesce tries it best to reduce shuffling across systems
rdd3 = rdd.coalesce(1)

print("partitions ", rdd3.getNumPartitions())
rdd3.glom().collect()

# COMMAND ----------

