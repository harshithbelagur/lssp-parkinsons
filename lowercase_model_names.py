import os

directory_path = "models/"
files = os.listdir(directory_path)

for file_name in files:
    # Construct the old and new file paths
    old_path = os.path.join(directory_path, file_name)
    new_name = file_name.lower()
    new_path = os.path.join(directory_path, new_name)
    
    # Rename the file
    os.rename(old_path, new_path)

print("All files have been renamed to lowercase.")