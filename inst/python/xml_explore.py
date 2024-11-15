import xmltodict

def xml_explore(file_or_content):
    """
    Parse the XML and report dataset-level name, row-level count, and row names once each.
    
    Parameters:
    - file_or_content: str (file path or raw XML content)
    
    Prints the discovered structure of the XML.
    """
    # Parse the input (either file path or raw XML content)
    if isinstance(file_or_content, str):
        if "\n" in file_or_content or "<" in file_or_content:  # Detect raw XML content
            try:
                data = xmltodict.parse(file_or_content)
            except Exception as e:
                print(f"Error parsing XML content: {e}")
                return
        else:  # Assume it's a file path
            try:
                with open(file_or_content, 'r') as file:
                    data = xmltodict.parse(file.read())
            except Exception as e:
                print(f"Error reading file: {e}")
                return
    else:
        print("Invalid input: must be a file path or raw XML content.")
        return

    # Verbose exploration
    print("Parsing completed. Exploring structure...\n")

    # Find dataset-level name, rows, and row names
    def report_structure(data):
        if not isinstance(data, dict):
            print("Invalid structure: Expected a dictionary at the root level.")
            return

        datasets = data.get('datasets', {}).get('dataset', [])
        if not datasets:
            print("No datasets found in the provided XML.")
            return

        # Ensure datasets is a list
        if not isinstance(datasets, list):
            datasets = [datasets]

        # Iterate over datasets
        for i, dataset in enumerate(datasets, start=1):
            dataset_name = dataset.get('@name', f"Unnamed Dataset {i}")
            print(f"Dataset Name: {dataset_name}")

            # Look for rows
            rows = dataset.get('row', [])
            if not rows:
                print("No rows found in this dataset.")
                continue

            if not isinstance(rows, list):  # Ensure it's a list
                rows = [rows]

            print(f"Number of Rows: {len(rows)}")

            # Report row names
            row_names = [row.get('rowname', 'Unnamed Row') for row in rows]
            print(f"Row Names: {row_names}")
            print("-----")

    # Start reporting structure
    report_structure(data)


