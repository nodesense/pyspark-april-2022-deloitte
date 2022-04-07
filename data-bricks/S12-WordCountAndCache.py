# Databricks notebook source
#  /FileStore/tables/2022-04-04/words.txt

# COMMAND ----------

from pyspark import StorageLevel


# COMMAND ----------

# how to remove special chars
# we will have only all ascii
def to_ascii(text):
    import re
    output = re.sub(r"[^a-zA-Z0-9 ]","",text)
    #print(output)
    return output

text = "prince, don't ;welcome"
to_ascii(text)

# COMMAND ----------

#  
#  /FileStore/tables/2022-04-04/words.txt
wordCountRdd = sc.textFile("/FileStore/tables/books/war_and_peace.txt")\
                 .map (lambda line: to_ascii(line))\
                  .map (lambda line: line.strip().lower())\
                 .map (lambda line: line.split(" "))\
                 .flatMap(lambda elements: elements)\
                 .filter (lambda word: word != "")\
                 .map (lambda word: (word, 1))\
                 .reduceByKey(lambda acc, value: acc + value)

# COMMAND ----------

# cache 
# cache function, will call persist function with MEMORY_ONLY option
# persist
wordCountRdd.cache() # the result shall be persisted, LAZY , when action applied first time, then the cache shall be used
#wordCountRdd.persist(StorageLevel.MEMORY_ONLY) # same as cache()
#wordCountRdd.persist(StorageLevel.DISK_ONLY) # store in disk
#wordCountRdd.persist(StorageLevel.MEMORY_AND_DISK) # store in memory and  disk
print("is cached ", wordCountRdd.is_cached)

# COMMAND ----------

# this action will load file from File IO, compute the result and take 10 elements from first partition
wordCountRdd.take(10) # we are using RDD, apply action

# COMMAND ----------

# reusing RDD [2] for soring in decending order
sortedRddDecending = wordCountRdd.sortBy(lambda kv: kv[1], ascending=False)
sortedRddDecending.take(20)


# COMMAND ----------

# reusing RDD [3] for soring in decending order
sortedRddAscending = wordCountRdd.sortBy(lambda kv: kv[1])
sortedRddAscending.take(20)

# COMMAND ----------

# to clear the cache if we no longer need it. 
# if unpersist not called, cache will be cleared on when stage completed
wordCountRdd.unpersist()

print("is cached ", wordCountRdd.is_cached)

# COMMAND ----------

