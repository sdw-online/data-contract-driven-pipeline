import pandas as pd 

def transform_data(df):
    print("\n>>> Transforming raw data ...")
    
    # Extract + transform relevant columns
    required_columns    = ["document", "name", "email", "phone", "len"]
    df                  = df[required_columns]

    # Convert columns to string data type    
    df["document"]      = df["document"].astype(str)
    df["name"]          = df["name"].astype(str)
    df["email"]         = df["email"].astype(str)
    df["phone"]         = df["phone"].astype(str)

    # Drop rows with NULLs
    df = df[pd.to_numeric(df["len"], errors="coerce").notnull()]  
    
    # Convert len to integer data type
    df["len"] = df["len"].astype(float).astype(int)  


    # Remove whitespace from name column
    df["name"]          = df["name"].str.strip()

    # Convert email characters to lowercase 
    df["email"]         = df["email"].str.lower()

    print("Transformation in Silver layer completed successfully")

    return df