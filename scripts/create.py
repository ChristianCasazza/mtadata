# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "flask",
#     "requests",
#     "duckdb",
# ]
# ///
# create.py


import subprocess
import os
import sys

def run_scripts():
    try:
        # Get the absolute path to the scripts folder
        scripts_dir = os.path.dirname(__file__)

        # Use the Python interpreter inside `uv`
        python_executable = sys.executable  

        # Always run 'createmetadata.py' and 'metadatadescriptions.py'
        print("Running createmetadata.py...")
        subprocess.run([python_executable, os.path.join(scripts_dir, "createmetadata.py")], check=True)

        print("Running metadatadescriptions.py...")
        subprocess.run([python_executable, os.path.join(scripts_dir, "metadatadescriptions.py")], check=True)

        # If 'app' is passed as an argument, also run 'app.py'
        if len(sys.argv) > 1 and sys.argv[1].lower() == "app":
            print("Running app.py...")
            subprocess.run([python_executable, os.path.join(scripts_dir, "app.py")], check=True)

        print("All scripts executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running scripts: {e}")

if __name__ == "__main__":
    run_scripts()
