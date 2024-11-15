import xmltodict
import duckdb
import pandas as pd

def xml_to_duckdb(filename, db_path):
    """
    Parse an XML file, explore its structure, and create tables in a DuckDB database.
    
    Parameters:
    - filename: str (path to the XML file)
    - db_path: str (path to the DuckDB database file)
    """
    try:
        # Read and parse the XML file
        with open(filename, 'r') as file:
            xml_content = file.read()
        data = xmltodict.parse(xml_content)
    except Exception as e:
        print(f"Error reading or parsing the XML file: {e}")
        return

    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    print(f"Connected to DuckDB database at: {db_path}")

    # Recursive function to explore and create tables
    def recursive_process(data, table_prefix="root"):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    # Convert the list of dictionaries to a DataFrame and create a table
                    if all(isinstance(item, dict) for item in value):
                        df = pd.DataFrame(value)
                        table_name = f"{table_prefix}_{key}"
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        print(f"Created table: {table_name} with {len(df)} rows.")
                    # Recurse into the list for further processing
                    for i, item in enumerate(value):
                        recursive_process(item, f"{table_prefix}_{key}[{i}]")
                elif isinstance(value, dict):
                    # Recurse into dictionaries
                    recursive_process(value, f"{table_prefix}_{key}")
                else:
                    # Skip non-dict, non-list values
                    pass
        elif isinstance(data, list):
            # Process lists that are not under a dictionary
            for i, item in enumerate(data):
                recursive_process(item, f"{table_prefix}[{i}]")

    # Start the recursive process
    recursive_process(data)

    # Close the connection
    conn.close()
    print("All tables created and database connection closed.")


