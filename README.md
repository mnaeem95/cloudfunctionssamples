# Google Cloud Functions Samples
Repository for various GCP tasks automated by Cloud Functions

# GCS to BigQuery to GCS
Simple data pipeline with a Google Cloud Storage trigger. Modeled after the data/queries used in [this](https://cloud.google.com/solutions/time-series/bigquery-financial-forex) tutorial for analyzing time series data. 

1. CSV is uploaded into GCS bucket
2. Table created in BigQuery
3. Data loaded into BigQuery
4. Runs query and exports to new table
5. New table data export to a CSV in GCS

# GCS to CloudSQL
This trigger was built to follow the GCS to BigQuery flow but can be run independently with some changes.

1. Cleansed data CSV added to GCS bucket (ignores the temp files that is created by BigQuery)
2. Creates a new table in the Cloud SQL database if it doesn't exist
3. Loads data into new table

# License
Apache 2.0

This is not an official Google product.
