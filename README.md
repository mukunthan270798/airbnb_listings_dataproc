# airbnb_listings_dataproc
This project processes airbnb listings csv file using pyspark in google cloud dataproc

## Spark data processing using DataProc

#### -> The script reads the csv file from GCS bucket
#### -> Drops the unnecessary columns
#### -> cast the columns to appropriate datatype
#### -> partitions the data by no of ppl property can accomodate
#### -> Write parquet files to GCS bucket

