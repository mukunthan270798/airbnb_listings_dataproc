# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 19:21:38 2022

@author: Mukunthan

This python script uses pyspark to read a csv file
Converts the raw csv file into processed csv by dropping the unnecessary columns and casting columns into appropriate data type
Partitions the file based on no of person the property can accomodate, write the files in parquet format into GCS bucket
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, DateType

spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

df = spark.read.options(header=True,nanValue="",inferSchema=True).csv("gs://airbnb_listing_analysis/raw_csv/listings.csv")

new_df=df.drop("host_location","host_has_profile_pic","minimum_nights","maximum_nights","latitude","longitude","amenities",\
    "instant_bookable","review_scores_accuracy","review_scores_cleanliness","review_scores_checkin",\
        "review_scores_communication","review_scores_location","review_scores_value")
    
listings=new_df.withColumn("host_since",to_date(col("host_since")))

listings_final = listings.withColumn("host_total_listings_count",col("host_total_listings_count").cast("int")).\
                withColumn("host_response_rate",col("host_response_rate").cast("float")).\
                withColumn("host_acceptance_rate",col("host_acceptance_rate").cast("float")).\
                withColumn("accommodates",col("accommodates").cast("int")).\
                withColumn("bedrooms",col("bedrooms").cast("int")).\
                withColumn("price",col("price").cast("int")).\
                withColumn("review_scores_rating",col("review_scores_rating").cast("int"))
                
listings_final.write.partitionBy("accommodates").mode("overwrite").parquet("gs://airbnb_listing_analysis/bucketed_parquet")