import os
import shutil
import subprocess

def setup_evidence_app():
    # Determine the project root and switch to it.
    project_root = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    os.chdir(project_root)
    
    # Change directory to "application"
    application_dir = os.path.join(project_root, "application")
    if not os.path.isdir(application_dir):
        print(f"Error: {application_dir} does not exist.")
        return
    os.chdir(application_dir)
    print(f"Changed directory to {application_dir}")
    
    # Clone the template using degit into a folder named "app"
    try:
        subprocess.run(["npx", "degit", "evidence-dev/template", "app"], check=True)
        print("Template cloned successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error cloning template: {e}")
        return
    except FileNotFoundError as e:
        print(f"Command not found: {e}")
        return
    
    # Change into the cloned folder "app"
    app_dir = os.path.join(application_dir, "app")
    if not os.path.isdir(app_dir):
        print(f"Error: {app_dir} was not created.")
        return
    os.chdir(app_dir)
    print(f"Changed directory to {app_dir}")
    
    # Delete the folder "sources/needful_things" if it exists
    needful_things_dir = os.path.join(app_dir, "sources", "needful_things")
    if os.path.isdir(needful_things_dir):
        shutil.rmtree(needful_things_dir)
        print(f"Deleted directory: {needful_things_dir}")
    else:
        print(f"Directory {needful_things_dir} not found; skipping deletion.")
    
    # Create a new folder in "sources" called "app"
    new_app_folder = os.path.join(app_dir, "sources", "app")
    os.makedirs(new_app_folder, exist_ok=True)
    print(f"Created new folder: {new_app_folder}")
    
    # Create the file connection.yaml inside the new folder with the specified content
    connection_file = os.path.join(new_app_folder, "connection.yaml")
    connection_content = """name: data
type: duckdb
options:
  filename: data.duckdb
"""
    with open(connection_file, "w") as f:
        f.write(connection_content)
    print(f"Created file: {connection_file}")
    
    # Run 'bun install' inside the cloned folder
    try:
        subprocess.run(["bun", "install"], check=True)
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
    except FileNotFoundError as e:
        print(f"Command not found: {e}")

if __name__ == "__main__":
    setup_evidence_app()
