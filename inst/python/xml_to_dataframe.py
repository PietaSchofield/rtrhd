import xmltodict
import pandas as pd

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

