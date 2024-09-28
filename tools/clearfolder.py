import os
import shutil

def clear_folder(folder_path):
    try:
        # Check if the folder exists
        if os.path.exists(folder_path):
            # List all files and subdirectories in the folder
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                try:
                    # Check if it is a file or directory
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove the file
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove the directory
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')
            print(f"All files and folders in '{folder_path}' have been cleared.")
        else:
            print(f"The folder '{folder_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while clearing the folder: {e}")

# Example usage:
folder_path = "/home/christianocean/oceandatachallengesdemo/data/testing"
clear_folder(folder_path)
