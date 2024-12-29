from dataclasses import dataclass

@dataclass(frozen=True)  # Make the class immutable 
class PIIDataSet:
    file_path: str
    file_name: str

    @staticmethod
    def select_dataset(layer: str, use_sample: bool):

        main_dataset    = PIIDataSet(file_path="data/1_bronze/pii_dataset.csv", file_name="pii_dataset.csv")
        sample_dataset  = PIIDataSet(file_path="data/1_bronze/sample_dataset.csv", file_name="sample_dataset.csv")

        if layer == "bronze":

            if use_sample:
                print("\nUsing 'sample' dataset for the bronze layer...")
                bronze_sample_dataset  = PIIDataSet(file_path="data/1_bronze/bronze_sample_dataset.csv", file_name="bronze_sample_dataset.csv")
                return bronze_sample_dataset
            
            else:
                print("\nUsing 'main' dataset for the bronze layer...")
                bronze_main_dataset    = PIIDataSet(file_path="data/1_bronze/pii_dataset.csv", file_name="pii_dataset.csv")
                return bronze_main_dataset
        
        elif layer == "silver":
            if use_sample:
                print("\nUsing 'sample' dataset for the silver layer...")
                silver_sample_dataset  = PIIDataSet(file_path="data/2_silver/silver_sample_dataset.csv", file_name="silver_sample_dataset.csv")
                return silver_sample_dataset
            
            else:
                print("\nUsing 'main' dataset for the silver layer...")
                silver_main_dataset    = PIIDataSet(file_path="data/1_bronze/pii_dataset.csv", file_name="pii_dataset.csv")
                return silver_main_dataset

        