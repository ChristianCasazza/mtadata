import os
import subprocess

def run_dbt_commands():
    try:
        # Change to the correct directory
        os.chdir("mta/transformations/dbt")
        
        # Run 'dbt run' command
        subprocess.run(["dbt", "run"], check=True)

        # Change back to the original directory
        os.chdir("../../..")

        print("Commands executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running dbt commands: {e}")
    except FileNotFoundError as e:
        print(f"Directory not found: {e}")

if __name__ == "__main__":
    run_dbt_commands()
