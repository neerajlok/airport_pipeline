# airport_pipeline
## Flight Data Processing Pipeline

This project implements a fully automated, scalable data engineering pipeline for processing daily flight data using AWS cloud services. The pipeline ingests raw flight data, performs a series of transformations, and updates an Amazon Redshift data warehouse with processed data to provide insights into performance metrics of airlines.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Data Flow](#data-flow)
- [Redshift Insights](#redshift-insights)
- [How to Run the Pipeline](#how-to-run-the-pipeline)


## Overview
This pipeline is designed to ingest daily CSV files containing flight data and transform them through AWS services. It is triggered upon the upload of a CSV file to an S3 bucket and follows an automated series of AWS Step Functions to:
1. Crawl and update the data catalog.
2. Perform transformations with AWS Glue.
3. Load transformed data into an Amazon Redshift data warehouse.
4. Update materialized views on Redshift to provide actionable insights.

## Architecture
![Architecture Diagram](https://github.com/neerajlok/airport_pipeline/blob/main/Airlines.png) 

### Key Components:
- **Amazon S3**: Stores raw CSV files containing flight data.
- **AWS Lambda**: Triggers the pipeline upon file upload to S3.
- **AWS Step Functions**: Orchestrates the pipeline workflow, triggering the AWS Glue Crawler and Glue Job in sequence.
- **AWS Glue Crawler**: Automatically updates the Data Catalog with metadata of the newly uploaded files.
- **AWS Glue Job**: Reads and processes data from the Fact table (S3) and Dimension table (Redshift), performs ETL operations, and writes data to Redshift.
- **Amazon Redshift**: Stores the transformed data and serves as the data warehouse for insights.
- **Amazon SNS**: Sends notifications based on the status of the Glue Job execution.

## Technologies Used
- **Amazon S3**: Data lake for flight data files.
- **AWS Lambda**: Serverless function to trigger pipeline execution.
- **AWS Step Functions**: Workflow orchestration.
- **AWS Glue**: Managed ETL service for data processing.
- **Amazon Redshift**: Data warehouse for analytics and reporting.
- **Amazon SNS**: Simple notification service for status updates.

## Data Flow
1. **Data Ingestion**: Daily CSV files containing fields (`Carrier`, `OriginAirportID`, `DestAirportID`, `DepDelay`, `ArrDelay`) are uploaded to an S3 bucket.
2. **Event Trigger**: The S3 upload triggers a Lambda function, which in turn starts an AWS Step Function.
3. **Crawling & Cataloging**: The Step Function triggers an AWS Glue Crawler to update the Data Catalog with the new CSV file's metadata.
4. **ETL Processing**: The Step Function triggers an AWS Glue Job, which:
   - Reads the flight data from S3 (Fact Table).
   - Joins with a Dimension Table from Amazon Redshift (containing airport details: `airport_id`, `city`, `state`, `name`).
   - Applies transformations, schema changes, and writes the output to Redshift.
   - Runs post-action scripts to update materialized views.
5. **Status Notification**: An SNS notification is sent based on the success or failure of the Glue Job.

## Redshift Insights
The materialized views in Amazon Redshift provide key insights, including:
- **Average Delay per Airline**
- **On-Time Performance**
- **High-Delay Flights**
- **Delays by state**
- **Most Frequent Cities**
- **Flight count by airport**

## How to Run the Pipeline
1. Upload the daily flight CSV file to the designated S3 bucket.
2. Ensure that AWS Step Functions, Lambda, Glue Crawler, and Glue Job are properly configured and have the necessary permissions.
3. The pipeline automatically runs based on the file upload event.
4. Review notifications from SNS for the job's success or failure status.
5. Query materialized views in Amazon Redshift for insights.


