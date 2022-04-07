# Databricks notebook source
# map
# flatMap 

# COMMAND ----------

rdd = sc.parallelize(range(0, 9))

# COMMAND ----------

#map is tranformation function, where it accept input, we can apply formula
# transformation, data conversion and return a new result
# new result shall be taken by spark for further iteration
mulBy10Rdd = rdd.map (lambda n: n * 10) # 0 * 10, 1*10, 2 * 10, .. 8 * 10
mulBy10Rdd.collect()

# COMMAND ----------

# List of List of elements
data = [
    [1,2,3], # element 0
    [4,5,6], # element 1
    [7,8,9]   # element 2
]

rdd = sc.parallelize(data)  

# COMMAND ----------

rdd.count() # 3

# COMMAND ----------

# flatMap
# flatten elements inside array into elements
# remove the list, project elements are records
# each element in the list will be 1 record
rddFlatMap = rdd.flatMap (lambda r: r)  # TRANSFORMATION
print ("Count ", rddFlatMap.count()) # ACTION METHOD
rddFlatMap.collect()

# COMMAND ----------

