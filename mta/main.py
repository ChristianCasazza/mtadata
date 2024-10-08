import os
from dotenv import load_dotenv
from dagster import materialize
from mta.definitions import defs  # Ensure you're importing from mta module

load_dotenv()

if __name__ == "__main__":
    result = materialize([defs.assets["fetch_and_save_weekly_data"]])
    if result.success:
        print("Pipeline ran successfully")
    else:
        print("Pipeline failed")
