# Databricks notebook source
upload_path = "/FileStore/tables/shared-uploads/population_data_upload"

dbutils.fs.mkdirs(upload_path)

# COMMAND ----------

checkpoint_path = '/FileStore/tables/population_data2/_checkpoints'
write_path = '/FileStore/tables/delta/population_data2'

# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .schema('city string, year int, population long') \
  .load(upload_path)

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that
# have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check,
# write the newly-uploaded files' data to the write_path location.
df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

