import xmltodict

def debug_keys(filename, path="root"):
    """
    Reads an XML file, parses it, and reports keys down to the second-to-last level.
    
    Parameters:
    - filename: str (path to the XML file)
    - path: str (used for recursion, leave default for initial call)
    """
    try:
        # Read and parse the XML file
        with open(filename, 'r') as file:
            xml_content = file.read()
        
        # Parse the XML content
        data = xmltodict.parse(xml_content)
    except Exception as e:
        print(f"Error reading or parsing the XML file: {e}")
        return
    
    # Internal recursive function to explore the structure
    def recursive_debug(data, path, level=0):
        if isinstance(data, dict):
            for key, value in data.items():
                print(f"Path: {path}/{key}, Type: {type(value)}")
                if isinstance(value, (dict, list)) and level < 1:  # Stop recursion at the row level
                    recursive_debug(value, f"{path}/{key}", level + 1)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                print(f"Path: {path}[{i}], Type: {type(item)}")
                if isinstance(item, (dict, list)) and level < 1:  # Stop recursion at the row level
                    recursive_debug(item, f"{path}[{i}]", level + 1)
        else:
            print(f"Path: {path}, Value: {data}")  # This only happens for single-level values

    # Start the recursion from the root
    recursive_debug(data, path)

