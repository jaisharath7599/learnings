#autoloader -- used to read the data from the cloud storage in incremental way and write to the bronze layer. 

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LeArNiNg").getOrCreate()

#to read a data from source
read_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv") # specify the format of the source data
        .option("cloudFiles.inferColumnTypes", "true") # infer the column types automatically from source data
        .option("cloudFiles.schemaLocation", "/path/to/schema") # specify the location to store the inferred schema
        .option("cloudFiles.schemaHints", "id int, name string, age int") # provide a schema hint to improve performance -- for some column we can manually provide the data type
        .load("/path/to/source/data") # specify the path to the source data
)

write_df = (read_df.writeStream
        .format("delta") # specify the format to write the data to the bronze layer
        .option("checkpointLocation", "/path/to/checkpoint") # specify the location to store the checkpoint data for fault tolerance
        .option("mergeSchema", "true") # allow schema evolution by merging the new schema with the existing schema in the bronze layer
        .outputMode("append") # specify the output mode to append the new data to the bronze layer
        .trigger(availableNow=True) # specify the trigger to process the data every 10 seconds
        .toTable("tableName") # specify the path to the bronze layer
)

#mointor the stream query
query = write_df.awaitTermination() # wait for the stream to finish processing
write_df.id # get the query id
write_df.status # get the current status of the stream query
write_df.name # get the name of the stream query
write_df.lastProgress # get the last progress of the stream query
write_df.stop() # stop the stream query

#trigger types:
#1. ProcessingTime [.trigger(processingTime("10 seconds"))]: process the data at a specified interval (e.g., every 10 seconds)
#2. Once [.trigger(once=True)]: process the data once and then stop
#3. AvailableNow [.trigger(availableNow=True)]: process all the available data and then stop
# by default, the trigger is set to process the data as soon as it arrives in the source location (i.e., micro-batch processing).

#output modes:
#1. Append: only new rows are added to the output table
#2. Update: only the rows that have changed since the last trigger are updated in the
#3. Complete: the entire output table is rewritten with each trigger, regardless of whether the data has changed or not.

#Bad records handling:
#if we got bad records in the source data, it will be stored in the _resuced_data column, where its json string columns, where in main column record will be null and in _resuced_data column we will get the bad record with the reason for the failure and also specify file where we got the bad record. 

#schema evolution:
#if we got new columns in the source data, it will fail the stream if we used mergeSchema option but when we rerun it will sucessfully process the data and add the new columns to the bronze layer. 

