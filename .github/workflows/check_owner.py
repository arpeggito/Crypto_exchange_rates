import sys
import yaml

def find_owner(data):
    """ Recursively search for 'Owner' key in a case-insensitive manner in the given data. """
    if isinstance(data, dict):
        # Convert all keys in the dictionary to lower case and check for 'owner'
        if 'owner' in map(str.lower, data.keys()):
            return True
        # Recurse into each value that is a dictionary or list
        for key in data:
            if find_owner(data[key]):
                return True
    elif isinstance(data, list):
        # If the value is a list, recurse into each element
        for item in data:
            if find_owner(item):
                return True
    return False

def check_owner(file_path):
    with open(file_path, 'r') as file:
        try:
            data = yaml.safe_load(file)
            if find_owner(data):
                print(f"Owner found in file {file_path}")
                return True
            else:
                print(f"Error: The file {file_path} does not contain the 'Owner' key.")
                return False
        except yaml.YAMLError as e:
            print(f"Error: Failed to parse YAML file {file_path}. Error: {e}")
            return False

if __name__ == "__main__":
    file_path = sys.argv[1]
    result = check_owner(file_path)
    if result:
        sys.exit(0)  # Exit with success status
    else:
        sys.exit(1)  # Exit with failure status
