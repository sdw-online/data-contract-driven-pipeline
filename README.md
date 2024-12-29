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
    - Check if silver S3 bucket exists✅
        - If not, create it✅
    - Read the raw data from the bronze bucket into a Pandas dataframe✅
        - If data doesn’t exist, re-run the bronze layer’s task✅
    - Apply necessary transformations (e.g. joins, filter, aggregations, renaming columns)✅
    - Write the transformed data into the silver bucket✅
    - Validate the transformed data against the silver-to-gold (S2G) data contract✅
        - If this doesn’t exist, define the validation rules and constraints in the silver-to-gold (S2G) data contract✅
        - Write a success message (to the console/logs) if validation passes✅
            - Otherwise, circuit break✅

- Gold layer✅
    - Check if gold bucket exists✅
        - If not, create it✅
    - Read the transformed and validated data from the silver layer into a pandas dataframe✅
        - If the data doesn’t exist, re-run the silver layer’s task✅
    - Check if the Postgres destination objects exist (database, schema, table)✅
        - If not, create them✅
        - If they do, delete and re-create them✅

- Postgres✅
    - Load data into target objects in Postgres✅
    - Run validation checks to confirm load was successful✅
        - Perform a row count✅
        - Perform a column count✅
        - Check column names✅

----

# Test

## End-to-end tests:


### 1. Test the Bronze_To_Silver data contract 


#### Expected outcome

- The 1-bronze-bucket S3 bucket is created 
- Validation fails at bronze layer

This is because we're using the bronze sample dataset which contains synthetic errors added by myself to deliberately break the workflow. 


#### Steps

- Set "TESTING_DATA_CONTRACT_MODE " to "Bronze_To_Silver" in pii_data_pipeline.py
- Manually trigger Airflow DAG
- Check the job once completed for any errors linked to data validation e.g. row count


#### Expectations met

-- Did this test return the expected outcomes (Y/N)?
EXPECTED_RESULTS_IN_TEST = "Y"


### 2. Test the Silver_To_Gold data contract

#### Expected outcome

- The 2-silver-bucket S3 bucket is created 
- Validation fails at silver layer

This is because we're using the silver sample dataset which contains synthetic errors added by myself to deliberately break the workflow. 


#### Steps

- Set "TESTING_DATA_CONTRACT_MODE " to "Silver_To_Gold" in pii_data_pipeline.py
- Manually trigger Airflow DAG
- Check the job once completed for any errors linked to data validation e.g. row count


#### Expectations met

-- Did this test return the expected outcomes (Y/N)?
EXPECTED_RESULTS_IN_TEST = ""

### 3. Test both data contracts (using the normal pii_dataset)

#### Expected outcome

- The 1-bronze-bucket, 2-silver-bucket, 3-gold-bucket S3 buckets are created 
- There are no validation errors at any stage



#### Steps

- Set "TESTING_DATA_CONTRACT_MODE " to "Both" in pii_data_pipeline.py
- Manually trigger Airflow DAG
- Check the job once completed for any errors linked to data validation e.g. row count


#### Expectations met

-- Did this test return the expected outcomes (Y/N)?
EXPECTED_RESULTS_IN_TEST = ""

### 4. Test both data contracts (By adding random errors in the pii_dataset)

#### Expected outcome

- There should be validation errors in the bronze or silver layer (Depending which data contract is being tested) 


#### Steps

- Set "TESTING_DATA_CONTRACT_MODE " to "Bronze_To_Silver" or "Silver_To_Gold" in pii_data_pipeline.py
- Backup the original pii_dataset.csv
- Manually add errors to the original pii_dataset.csv
- Manually trigger Airflow DAG
- Check the job once completed for any errors linked to data validation e.g. row count



#### Expectations met

-- Did this test return the expected outcomes (Y/N)?
EXPECTED_RESULTS_IN_TEST = ""