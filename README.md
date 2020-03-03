# LeafLink Interview

This codebase is designed to load "impression" and "click" files from S3 into Redshift. Via the psycopg2 package, leaflink_s3_to_redshift.py executes DDL commands for creating tables/views in Redshift and COPY commands to load data from S3. Environment specific information is read from config.ini file.

## Design
Impression and click files are made up of multilevel json data containing various IDs and details about the user interaction. Each file is initially loaded into its staging table, impressions_staging or clicks_staging, via a postgres COPY command. The COPY command references "jsonpath" files in S3 which map first-level json key/value pairs to respective columns in the staging tables.

Once data is loaded into the staging tables, impressions and clicks views are able to parse second-level json key/values from User and Device into their own columns. Pertinent info can then be queried from the impressions and clicks views.

## Installation
In order to run successfully, Redshift must allow inbound traffic from the environment where leaflink_s3_to_redshift.py is being run. Additionally, the Redshift cluster must have IAM and role to read from the S3 bucket containing impressions and clicks data. 

The config.ini file must be updated with environment the values specific to your environment. Below are details on what values are required in each field.

#### Redshift
dbname - name of Redshift database

host - Redshift cluster host

port - connection port for Redshift cluster

user - Redshift user

password - Redshift password

iam_role - IAM role with S3 access

#### S3
impressions_jsonpath - S3 path for impressions_jsonpath.json file (bucket and prefix)

clicks_jsonpath - S3 path for clicks_jsonpath.json file (bucket and prefix)

data_bucket - bucket containing impressions and clicks data

## Execution
To execute run the following command: "python leaflink_s3_to_redshift.py"
