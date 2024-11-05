import xmltodict
import csv

def xml_to_csv(xml_file, csv_file, data, rows, row):
  # Open the XML file and parse it with xmltodict
  with open(xml_file, 'r') as file:
    xml_content = xmltodict.parse(file.read())

  # Access the list of <AMPP> rows within the root <AMPPS> element
  rows = xml_content[data][rows][row]

  # Ensure rows is a list, even if there's only one <AMPP> element
  if isinstance(rows, dict):
    rows = [rows]

  # Collect all unique keys across all rows to ensure all fields are included
  all_fieldnames = set()
  for row in rows:
    all_fieldnames.update(row.keys())
  all_fieldnames = list(all_fieldnames)  # Convert to list for DictWriter

  # Write data to CSV
  with open(csv_file, 'w', newline='') as csvfile:
    csvwriter = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
        
    # Write the header row
    csvwriter.writeheader()
        
    # Write each <AMPP> element as a row in the CSq
    for row in rows:
      csvwriter.writerow(row)

