import subprocess
import os

def run_command(command, cwd=None):
    """Run a command in the terminal."""
    result = subprocess.run(command, shell=True, cwd=cwd, text=True)
    if result.returncode != 0:
        raise Exception(f"Command failed: {command}")
    return result

def main():
    try:
        # Run python createlake.py
        print("Running createlake.py...")
        run_command("python createlake.py")

        # Change to the mta/transformations/dbt directory and run dbt
        print("Changing to mta/transformations/dbt directory and running dbt...")
        run_command("dbt run", cwd="mta/transformations/dbt")

        # Return to the original directory
        print("Returning to the original directory...")
        os.chdir("../../..")

        print("All commands completed successfully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
