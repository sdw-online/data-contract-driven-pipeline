import os
from cryptography.fernet import Fernet

"""
How to use:

1. Run script 
2. Add fernet key to .env file
3. Add webserver secret key to .env file


Run the end-to-end data workflow in Airflow to test if these keys work
"""

def generate_keys():

    # --- 1. Generate Fernet key
    fernet_key = Fernet.generate_key()
    print(f"New Fernet Key: {fernet_key.decode()}")

    # --- 2. Generate secret key for airflow-webserver
    secret_key = os.urandom(32).hex()
    print(f"New Webserver Secret Key: {secret_key}")

if __name__ == "__main__":
    generate_keys()


