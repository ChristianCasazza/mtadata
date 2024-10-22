import subprocess

def run_scripts():
    try:
        # Run 'createlake.py'
        print("Running createlake.py...")
        subprocess.run(["python", "createlake.py"], check=True)

        # Run 'createmetadata.py'
        print("Running createmetadata.py...")
        subprocess.run(["python", "createmetadata.py"], check=True)

        # Run 'metadatadescriptions.py'
        print("Running metadatadescriptions.py...")
        subprocess.run(["python", "metadatadescriptions.py"], check=True)

        print("All scripts executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running scripts: {e}")

if __name__ == "__main__":
    run_scripts()
