import psycopg2
from psycopg2.extras import execute_values

# Constants
DATABASE_NAME   = "prod_db"
SCHEMA_NAME     = "gold_layer"
TABLE_NAME      = "pii_records_tbl"


# SQL queries 
CREATE_TABLE_QUERY = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            document    TEXT NOT NULL,
            name        TEXT NOT NULL,
            email       TEXT,
            phone       TEXT,
            len         INT
            );
"""

CREATE_SCHEMA_QUERY = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}
    ;
"""


TRUNCATE_TABLE_QUERY = f"""
    TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}
    ;
"""

INSERT_DATA_QUERY = f"""
    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (document, name, email, phone, len)
    VALUES  %s
    ;
"""

GOLD_VALIDATION_QUERY = f"""
    SELECT * 
    FROM {SCHEMA_NAME}.{TABLE_NAME}
    ;
"""

def test_postgres_connection(postgres_config):
    try:
        print(">>> Attempting to connect to Postgres...")
        postgres_connection = psycopg2.connect(**postgres_config)
        postgres_connection.close()

        print("Connected to Postgres successfully")
    except Exception as e:
        print(f"[ERROR] - Failed to connect to PostgreSQL: {e} ")

def initialize_postgres(postgres_config, truncate_table=True):
    
    
    try:
        
        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()


        # Create schema 
        cursor.execute(CREATE_SCHEMA_QUERY)

        # Create table
        cursor.execute(CREATE_TABLE_QUERY)
        

        # Truncate table if it contains data (i.e. truncate + load pattern)
        if truncate_table:
            cursor.execute(TRUNCATE_TABLE_QUERY)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Postgres schema '{SCHEMA_NAME}' and '{TABLE_NAME}' verified/created successfully")
    
    except Exception as e:
        raise RuntimeError(f"[ERROR] - Unable to verify or create Postgres objects: {e}")



def load_data_into_postgres(postgres_config, df):

    try:
        conn                    = psycopg2.connect(**postgres_config)
        cursor                  = conn.cursor()
        list_of_pii_records     = df.values.tolist()
        
        # Use `execute_values` for batch inserts
        execute_values(cursor, INSERT_DATA_QUERY, list_of_pii_records)
        conn.commit()
        cursor.close()
        conn.close()
        
        print(">>> Data successfully loaded into Postgres.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] - Failed to load data into Postgres: {e}")


def validate_postgres_load(postgres_config, expected_row_count, expected_columns):
    
    try:
        conn                = psycopg2.connect(**postgres_config)
        cursor              = conn.cursor()
        cursor.execute(GOLD_VALIDATION_QUERY)
        rows                = cursor.fetchall()
        actual_row_count    = len(rows)

        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        # Log row and column counts for debugging
        print(f"Postgres row count:     {actual_row_count},    Expected: {expected_row_count}")
        print(f"Postgres columns:       {columns},      Expected: {expected_columns}")

        # Validate row count
        if actual_row_count != expected_row_count:
            raise ValueError(
                f"[ERROR] - Row count mismatch in Postgres: Expected {expected_row_count}, Found {actual_row_count}"
            )

        # Validate column names
        if columns != expected_columns:
            raise ValueError(
                f"[ERROR] - Column mismatch in Postgres: Expected {expected_columns}, Found {columns}"
            )

        print("Postgres validation passed successfully.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] - Postgres validation failed: {e}")
