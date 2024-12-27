# data-contract-pipeline-test-v2
Automating data workflows with data contracts (v2)




Website for dataset: "https://www.kaggle.com/datasets/alejopaullier/pii-external-dataset"




# Pseudocode

- Setup ✅ 
    - Import dependencies ✅ 
    - Import environment variables from .env file ✅
    - Get CSV file from Kaggle ✅ 
    - Create a sample dataset for testing purposes ✅ 


- Bronze layer
    - Check if bronze S3 bucket exists ✅
        - If not, create it ✅ 
    - Check if source CSV file exists in bucket✅
        - If not, upload to bronze bucket✅
    - Read the data from the bronze bucket into the pandas dataframe✅
    - Validate the raw data against the bronze-to-silver (B2S) data contract ✅ 
        - If this doesn’t exist, define the validation rules and constraints in the bronze-to-silver (B2S) data contract✅
            - Check the number of rows✅
            - Check the number of columns✅
    - If validation passes, proceed (write a success message to the console + logs)✅
        - Otherwise, circuit break the data pipeline (log and exit)✅

- Silver layer
    - Check if silver S3 bucket exists
        - If not, create it
    - Read the raw data from the bronze bucket into a Pandas dataframe
        - If data doesn’t exist, re-run the bronze layer’s task
    - Apply necessary transformations (e.g. joins, filter, aggregations, renaming columns)
    - Write the transformed data into the silver bucket
    - Validate the transformed data against the silver-to-gold (S2G) data contract
        - If this doesn’t exist, define the validation rules and constraints in the silver-to-gold (S2G) data contract
        - Write a success message (to the console/logs) if validation passes
            - Otherwise, circuit break

- Gold layer
    - Check if gold bucket exists
        - If not, create it
    - Read the transformed and validated data from the silver layer into a pandas dataframe
        - If the data doesn’t exist, re-run the silver layer’s task
    - Check if the Postgres destination objects exist (database, schema, table)
        - If not, create them
        - If they do, delete and re-create them

- Postgres
    - Load data into target objects in Postgres
    - Run validation checks to confirm load was successful
        - Perform a row count
        - Perform a column count
        - Check column names