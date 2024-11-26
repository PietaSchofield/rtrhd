import glob
import xmltodict
import pandas as pd
import os

def xml_to_dataframe(xml_file, dataset_key, rows_key, rowname_key):
    """
    Parse an XML file with flexible keys and return a pandas DataFrame.

    Parameters:
    - xml_file (str): Path to the XML file.
    - dataset_key (str): Key for the Dataset level.
    - rows_key (str): Key for the Rows level (if applicable).
    - rowname_key (str): Key for the Rowname level.

    Returns:
    - pandas.DataFrame: Extracted data as a DataFrame.
    """
    with open(xml_file, 'r') as file:
        # Parse the XML into a dictionary
        xml_content = xmltodict.parse(file.read())

    # Navigate to the dataset level
    dataset = xml_content.get(dataset_key, {})

    # Determine rows based on the presence of rows_key
    rows = None
    if rows_key in dataset:
        # If rows_key exists, navigate deeper
        rows = dataset[rows_key].get(rowname_key, [])
    else:
        # Otherwise, assume rowname_key is directly in dataset
        rows = dataset.get(rowname_key, [])

    # Ensure rows is a list
    if isinstance(rows, dict):  # Single row case
        rows = [rows]

    # Convert to pandas DataFrame
    df = pd.DataFrame(rows) if rows else pd.DataFrame()

    # Log details for debugging
    print(f"Dataset key: {dataset_key}")
    print(f"Rows key: {rows_key}")
    print(f"Rowname key: {rowname_key}")
    print(f"Number of rows extracted: {len(rows)}")
    print(f"DataFrame preview:\n{df.head()}")

    return df

def process_files_from_xml(config_file, search_directory):
    """
    Process XML files based on a stub and a directory, using configurations from an XML file.

    Parameters:
    - config_file (str): Path to the configuration XML file containing stubs and keys.
    - search_directory (str): Directory to search for files.

    Returns:
    - None: Processes and prints DataFrames for each file found.
    """
    # Read the configuration XML
    with open(config_file, 'r') as file:
        config_data = xmltodict.parse(file.read())

    # Extract the file entries
    files = config_data['files']['file']
    
    # Ensure it's a list (even if there's only one file)
    if isinstance(files, dict):
        files = [files]
    
    # Loop through each file entry
    for entry in files:
        stub = entry['file_name']  # Use stub instead of full file name
        dataset_key = entry['dataset_key']
        rows_key = entry['rows_key']
        rowname_key = entry['rowname_key']
        
        # Find files matching the stub in the directory recursively
        matching_files = glob.glob(os.path.join(search_directory, f"**/*{stub}*"), recursive=True)
        
        if not matching_files:
            print(f"No matching files found for stub: {stub}")
            continue
        
        # Process the first matching file (you can extend this for multiple matches)
        file_to_process = matching_files[0]
        print(f"Processing file: {file_to_process}")
        print(f"Dataset Key: {dataset_key}, Rows Key: {rows_key}, Rowname Key: {rowname_key}")
        
        # Call your existing xml_to_dataframe function
        df = xml_to_dataframe(
            xml_file=file_to_process,
            dataset_key=dataset_key,
            rows_key=rows_key,
            rowname_key=rowname_key
        )
        
        print(f"Extracted DataFrame for {file_to_process}:\n{df.head()}")


