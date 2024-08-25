def extract_keys_from_string(file_content):
    """
    Extracts JSON keys from the provided log file content.
    
    Parameters:
    - file_content (str): Content of the log file.
    
    Returns:
    - keys (set): A set of extracted keys.
    """
    keys = set()
    for line in file_content.splitlines():
        if 'Extracted data keys:' in line:
            # Extract keys from the line
            start = line.find('{') + 1
            end = line.rfind('}')
            keys_str = line[start:end]
            keys_list = keys_str.split(', ')
            keys.update([key.strip("'") for key in keys_list])
    return keys

def reconstruct_json_from_keys(key_set):
    """
    Reconstructs a JSON object from a set of keys with '__' indicating sub-levels.
    Adds placeholder values 'BORK' for each key.
    
    Parameters:
    - key_set (set): A set of keys representing paths in a JSON structure, separated by '__'.
    
    Returns:
    - json_obj (dict): The reconstructed JSON object with placeholder values.
    """
    json_obj = {}
    for key in key_set:
        parts = key.split('__')
        current_level = json_obj
        for part in parts[:-1]:
            if part not in current_level or not isinstance(current_level[part], dict):
                current_level[part] = {}  # Ensure it's a dictionary
            current_level = current_level[part]
        current_level[parts[-1]] = "BORK"
    return json_obj

def pretty_print_json(json_obj):
    """
    Pretty prints a JSON object.
    
    Parameters:
    - json_obj (dict): The JSON object to print.
    """
    import json
    print(json.dumps(json_obj, indent=4))

# Example usage

# Specify your file path
file_path = 'src/framework/processing/py/port/example.json'  # Replace with your actual file path

# Read the file content outside the function
with open(file_path, 'r') as file:
    file_content = file.read()

# Extract keys from the file content
extracted_keys = extract_keys_from_string(file_content)

# Reconstruct the JSON
reconstructed_json = reconstruct_json_from_keys(extracted_keys)

# Pretty print the JSON
pretty_print_json(reconstructed_json)
