# Databricks notebook source
# Partitions
# subset of data or chunk of data, to be processed by the executor program parallelly
# partitions data are immutable, means once created, we cannot change them
# partitiosn are distributed across cluster
# partitions are created inside EXECUTOR not INSIDE DRIVER program
# the drivers need to collect or take like function to collect data from partitions from executors to driver
# many Input functions has default partitions which can be configured or we can explicitly state how many partitiosn needed while loading data

# COMMAND ----------

rdd = sc.parallelize(range(0, 20)) # numebrs from 0 to 19


# COMMAND ----------

print("default parallism", sc.defaultParallelism) # derived from spark master local[8]
print ("default min partition ", sc.defaultMinPartitions) # used with hadoop or file reading
rdd.getNumPartitions() # parallelize func uses defaultParalleism to create partitions

# COMMAND ----------

# get data from all the partitions, merge them into single collection
result = rdd.collect() # ACTION
print(result)

# COMMAND ----------

# glom() collect data from individual partitions, return results as list of elements for each partition

result = rdd.glom().collect() # ACTION
result # print multiple list, each list contain one partition data

# COMMAND ----------

# take ACTION func, must  be used in place of collect() as collect is expensive
# take read elements based on limit, starting from 0th partition
result = rdd.take(3) # take 0, 1 from 0th partition, read 2 from 1st partition
result

# COMMAND ----------

rdd2 = sc.parallelize(range(0, 20), 2) # load numbers from 0 to 19 into 2 partitions
rdd2.getNumPartitions()

# COMMAND ----------

rdd2.glom().collect()

# COMMAND ----------

sc # ?Master local[8] - default parallism

# COMMAND ----------

sc.parallelize([0,1]).glom().collect()

# COMMAND ----------

