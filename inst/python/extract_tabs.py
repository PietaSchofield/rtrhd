# Extract tables from pdf and write to excel

import pdfplumber
import pandas as pd

def extract_tables_to_excel(pdf_path, output_excel_path):
    """
    Extracts tables from a PDF and saves each as a sheet in an Excel workbook.

    Parameters:
        pdf_path (str): Path to the PDF file.
        output_excel_path (str): Path to save the Excel workbook.
    """
    # Initialize a dictionary to hold data for Excel
    excel_sheets = {}

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            # Extract tables from the current page
            page_tables = page.extract_tables()

            for table_index, table in enumerate(page_tables):
                # Convert the table to a DataFrame
                df = pd.DataFrame(table)
                
                # Create a unique sheet name
                sheet_name = f"Page_{page_num}_Table_{table_index + 1}"
                
                # Store the table in the dictionary
                excel_sheets[sheet_name] = df

    # Write all sheets to an Excel workbook
    with pd.ExcelWriter(output_excel_path, engine="xlsxwriter") as writer:
        for sheet_name, df in excel_sheets.items():
            # Limit sheet name to 31 characters to meet Excel requirement
            writer.sheets[sheet_name[:31]] = writer.book.add_worksheet(sheet_name[:31])
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    
    print(f"Tables successfully saved to {output_excel_path}")
