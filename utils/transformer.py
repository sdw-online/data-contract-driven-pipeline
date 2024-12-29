def transform_data(df):
    print("\n>>> Transforming raw data ...")
    
    # Extract + transform relevant columns
    required_columns    = ["document", "name", "email", "phone", "len"]
    df                  = df[required_columns]
    
    # Remove whitespace from name column
    df["name"]          = df["name"].str.strip()

    # Convert email characters to lowercase 
    df["email"]         = df["email"].str.lower()

    print("Transformation in Silver layer completed successfully")

    return df