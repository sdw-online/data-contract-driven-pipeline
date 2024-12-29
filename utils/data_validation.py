import os
import json

def validate_data(df, contract_path):
    
    if not os.path.exists(contract_path):
        raise FileNotFoundError(f"[ERROR] - Data contract '{contract_path}' not found.")

    with open(contract_path, "r") as file:
        print(f"Contract path: {contract_path}")
        contract = json.load(file)

    contract_name       = contract.get("contract_name", "")
    validation_rules    = contract.get("validation_rules", {})
    schema              = contract.get("schema", {})
    expected_columns    = schema.get("columns", [])

    # Bronze-to-Silver: data validation (structural)
    if "BronzeToSilver" in contract_name:
        print("\n>>> Performing bronze-to-silver data validation checks...")

        # Validate row count
        expected_min_row_count  = validation_rules.get("row_count_min", 0)
        actual_row_count        = len(df)
        
        if actual_row_count < expected_min_row_count:
            raise ValueError(
                f"[ERROR] - Row count validation failed: Expected at least {expected_min_row_count} rows, but found {actual_row_count}."
            )

        # Validate column count
        expected_col_count = validation_rules.get("column_count", len(expected_columns))
        actual_col_count = len(df.columns)
        if actual_col_count != expected_col_count:
            raise ValueError(
                f"[ERROR] - Column count validation failed: Expected {expected_col_count} columns, but found {actual_col_count}."
            )

        print("Validation passed successfully for Bronze-to-Silver contract.")
        return


    # Silver-to-Gold data validation (content)
    if "SilverToGold" in contract_name:
        print("\n>>> Performing Silver-to-Gold data validation checks...")

        # Validate row count
        expected_min_row_count = validation_rules.get("row_count_min", 0)
        actual_row_count = len(df)
        if actual_row_count < expected_min_row_count:
            raise ValueError(
                f"[ERROR] - Row count validation failed: Expected at least {expected_min_row_count} rows, but found {actual_row_count}."
            )

        # Validate column count
        expected_col_count = validation_rules.get("column_count", len(expected_columns))
        actual_col_count = len(df.columns)
        if actual_col_count != expected_col_count:
            raise ValueError(
                f"[ERROR] - Column count validation failed: Expected {expected_col_count} columns, but found {actual_col_count}."
            )

        # Validate column names, types, and constraints
        for col_spec in expected_columns:
            col_name = col_spec["name"]
            col_type = col_spec["type"]
            constraints = col_spec.get("constraints", {})


            # Check for missing columns
            list_of_actual_columns = df.columns
            if col_name not in list_of_actual_columns:
                raise ValueError(f"[ERROR] - Missing required column: '{col_name}'.")

            # Validate data type
            is_column_type_string = pd.api.types.is_string_dtype(df[col_name])

            if col_type == "string" and not is_column_type_string:
                raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'string'.")
            

            is_column_type_integer = pd.api.types.is_integer_dtype(df[col_name])

            if col_type == "integer" and not is_column_type_integer:
                raise TypeError(f"[ERROR] - Column '{col_name}' should be of type 'integer'.")


            # Validate constraints
            has_null_values = df[col_name].isnull().any()
            if constraints.get("not_null") and has_null_values:
                raise ValueError(f"[ERROR] - Column '{col_name}' contains NULL values.")
            

            has_duplicate_values = df[col_name].duplicated().any()
            if "unique" in constraints and has_duplicate_values:
                raise ValueError(f"[ERROR] - Column '{col_name}' contains duplicate values.")
            


        print("Full validation passed successfully for Silver-to-Gold contract.")
        return

    # Return error if we don't know what contract type it is
    raise ValueError(f"[ERROR] - Unknown contract type: '{contract_name}'.")