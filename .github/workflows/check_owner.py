# check_owner.py
import sys
import yaml

def check_owner(file_path):
    with open(file_path, 'r') as file:
        try:
            data = yaml.safe_load(file)
            if 'Owner' not in data:
                print(f"Error: The file {file_path} does not contain the 'Owner:' key.")
                return False
            return True
        except yaml.YAMLError as e:
            print(f"Error: Failed to parse YAML file {file_path}. Error: {e}")
            return False

if __name__ == "__main__":
    file_path = sys.argv[1]
    if check_owner(file_path):
        print("The file contains the 'Owner:' key.")
        sys.exit(0)
    else:
        sys.exit(1)
