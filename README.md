# Simple Data Ingestion Pipeline using Python & AWS

## Overview
This project implements a serverless data ingestion pipeline using Python and AWS. Raw CSV data is uploaded to Amazon S3, automatically processed by AWS Lambda, validated, and stored into processed and error zones.

## Architecture
- Amazon S3 (Raw, Processed, Error)
- AWS Lambda (Validation & Processing)
- Amazon CloudWatch (Logging)
- AWS IAM (Security)

## Data Flow
1. Upload CSV to S3 raw/
2. S3 triggers Lambda
3. Lambda validates and processes data
4. Output stored in processed/ and error/
5. Logs sent to CloudWatch

## Technologies
- Python
- AWS S3
- AWS Lambda
- AWS IAM
- CloudWatch

## Author
Badal Kumar
