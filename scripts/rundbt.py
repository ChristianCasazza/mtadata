import os
import subprocess

def run_dbt_commands():
    try:
        # Change to the correct directory relative to the scripts folder
        os.chdir(os.path.join(os.path.dirname(__file__), '..', 'mta', 'transformations', 'dbt'))
        
        # Run 'dbt run' command
        subprocess.run(["dbt", "run"], check=True)

        # Change back to the original directory
        os.chdir(os.path.dirname(__file__))

        print("Commands executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running dbt commands: {e}")
    except FileNotFoundError as e:
        print(f"Directory not found: {e}")

if __name__ == "__main__":
    run_dbt_commands()
