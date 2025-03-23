import os
import shutil
import subprocess

def setup_evidence_app(app_name="app"):
    # Determine project root. If __file__ is defined, assume the script is in the project root.
    project_root = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    
    # Always start at the project root.
    os.chdir(project_root)
    
    # Change directory to "application"
    application_dir = os.path.join(project_root, "application")
    if not os.path.isdir(application_dir):
        print(f"Error: {application_dir} does not exist.")
        return
    os.chdir(application_dir)
    print(f"Changed directory to {application_dir}")
    
    # Check if seeds exist in "application/seed/<app_name>" (i.e. both sources and pages)
    seed_app_dir = os.path.join(application_dir, "seed", app_name)
    seeds_exist = (os.path.exists(os.path.join(seed_app_dir, "sources")) and 
                   os.path.exists(os.path.join(seed_app_dir, "pages")))
    
    if not seeds_exist:
        print(f"Seed files for '{app_name}' not found in {seed_app_dir}. Proceeding with seed update...")
        # Delete the folder "app/sources/needful_things" if it exists
        needful_things_dir = os.path.join(application_dir, "app", "sources", "needful_things")
        if os.path.isdir(needful_things_dir):
            shutil.rmtree(needful_things_dir)
            print(f"Deleted directory: {needful_things_dir}")
        else:
            print(f"Directory {needful_things_dir} not found; skipping deletion.")
        
        # Create a new folder called <app_name> inside "app/sources"
        new_seed_folder = os.path.join(application_dir, "app", "sources", app_name)
        os.makedirs(new_seed_folder, exist_ok=True)
        print(f"Created new folder: {new_seed_folder}")
        
        # Create the file connection.yaml in the new folder (with default content)
        connection_file = os.path.join(new_seed_folder, "connection.yaml")
        with open(connection_file, "w") as f:
            f.write("# Connection configuration\n")
        print(f"Created file: {connection_file}")
    else:
        print(f"Seed files for '{app_name}' exist. Skipping seed update.")
    
    # Run the command to clone the template
    try:
        # Clones into a folder called "app" (as in your original command)
        subprocess.run(["npx", "degit", "evidence-dev/template", "app"], check=True)
        print("Template cloned successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error cloning template: {e}")
        return
    except FileNotFoundError as e:
        print(f"Command not found: {e}")
        return
    
    # Change into the cloned folder and run 'bun install'
    app_dir = os.path.join(application_dir, "app")
    if not os.path.isdir(app_dir):
        print(f"Error: {app_dir} was not created.")
        return
    os.chdir(app_dir)
    print(f"Changed directory to {app_dir}")
    
    try:
        subprocess.run(["bun", "install"], check=True)
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
    except FileNotFoundError as e:
        print(f"Command not found: {e}")

# Example usage:
if __name__ == "__main__":
    setup_evidence_app("app")
