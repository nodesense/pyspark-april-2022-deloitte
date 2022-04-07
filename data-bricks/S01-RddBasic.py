# Databricks notebook source
# This is notebook, where we can write python, r, sql, scala, shell code here
# this block is called one cell
# cell can be executed using Shift + Enter key
# the last line in the cell is displayed automatically or just use print statement

10 + 20  # Shift + enter

# COMMAND ----------

# spark context
# known as sc, already initialized by databricsk notebook. 
sc # shift enter

# COMMAND ----------

# CREATION OF RDD using spark context

# type sc.<TAB><TAB><TAB> - intellisence
# loading data from driver into RDD/exeuctor
# lazy evaluation, while we run this code, no partition, no executors are used
rdd = sc.parallelize( [1,2,3,4,5,6,7,8,9] )

# COMMAND ----------

# TRANSFORMATION 
# logic applied on RDD data, map, filter, sort, join, etc
# LAZY evaluation, no logic applied immediately until we apply ACTION
# lambda func is called for every number from 1 to 9, this lambda return true for even, false for odd number
evenRdd = rdd.filter (lambda n : n % 2 == 0)

# COMMAND ----------

# ACTION
# action is applied on RDD, to the get the final output
# ACTION will create JOB
# JOB shall convert RDD Tree into RDD Graph
# RDD GRaph basically split into STAGEs
# Each STAGE shall have 1 or more TASK, basically scheduled and managed by Task Scheduler
# each task shall be submitted to Spark Executor, parallelly executed against partition
# finally action method MAY or MAY NOT collect results back to driver based on API

# collect - collect results to driver from executors
# NOT LAZY Evaluation, this allocate memory for Partitions, submit task to executor, perform logics, collect result
result = evenRdd.collect()

print(result)

# COMMAND ----------

# 2 mins, DIY - create a Transformation function for odd number and print odd numbers
oddRdd = rdd.filter (lambda n: n % 2 == 1) # TRANSFORMATION, lazy
oddRdd.collect() # Action, action create obj, execute logics

# COMMAND ----------

