import subprocess
import os
import sys

def run_scripts():
    try:
        # Get the absolute path to the scripts folder
        scripts_dir = os.path.dirname(__file__)

        # Always run 'createlake.py' first unless the 'app' option is passed
        if len(sys.argv) <= 1 or sys.argv[1].lower() != "app":
            print("Running createlake.py...")
            subprocess.run(["python", os.path.join(scripts_dir, "createlake.py")], check=True)

        # Always run 'createmetadata.py' and 'metadatadescriptions.py'
        print("Running createmetadata.py...")
        subprocess.run(["python", os.path.join(scripts_dir, "createmetadata.py")], check=True)

        print("Running metadatadescriptions.py...")
        subprocess.run(["python", os.path.join(scripts_dir, "metadatadescriptions.py")], check=True)

        # Check if additional arguments are provided
        if len(sys.argv) > 1:
            option = sys.argv[1].lower()

            # Handle 'dbt' option
            if option == "dbt":
                print("Running rundbt.py...")
                subprocess.run(["python", os.path.join(scripts_dir, "rundbt.py")], check=True)

            # Handle 'app' option
            elif option == "app":
                print("Running app.py...")
                subprocess.run(["python", os.path.join(scripts_dir, "app.py")], check=True)

            # Handle 'full' option (run both dbt and app)
            elif option == "full":
                print("Running rundbt.py...")
                subprocess.run(["python", os.path.join(scripts_dir, "rundbt.py")], check=True)
                print("Running app.py...")
                subprocess.run(["python", os.path.join(scripts_dir, "app.py")], check=True)

        print("All scripts executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running scripts: {e}")

if __name__ == "__main__":
    run_scripts()
