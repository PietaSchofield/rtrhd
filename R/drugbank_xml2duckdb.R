#' Parse DrugBank XML to DuckDB (long format, batch inserts)
#'
#' @param xml_file Path to full_database.xml
#' @param duckdb_file Path to output .duckdb file
#' @param batchsize Number of drugs to batch insert (default 1000)
#' @param overwrite Overwrite existing DuckDB file (default FALSE)
#' @export
drugbank_xml2duckdb <- function(xml_file, duckdb_file, batchsize = 1000, overwrite = FALSE) {

  if (file.exists(duckdb_file) && !overwrite) {
    message("DuckDB file already exists, skipping.")
    return(invisible(NULL))
  }
  
  # Ensure Python packages are available
  reticulate::py_require(c("duckdb", "pandas"))
  unlink(duckdb_file)
  
  # Glue Python code
  pystring <- paste(c(
    "import xml.etree.ElementTree as ET",
    "import duckdb",
    "import pandas as pd",
    "",
    "# Helper to strip XML namespaces",
    "def strip_ns(tag):",
    "    return tag.split('}', 1)[1] if '}' in tag else tag",
    "",
    "# Flatten XML recursively into long format",
    "def flatten_xml_long(element, drug_id=None, prefix=''):",
    "    records = []",
    "    for child in element:",
    "        tag = f'{prefix}{strip_ns(child.tag)}'",
    "        if len(child):",
    "            records.extend(flatten_xml_long(child, drug_id=drug_id, prefix=tag + '_'))",
    "        else:",
    "            value = child.text if child.text else ''",
    "            records.append({'drug_id': drug_id, 'key': tag, 'value': value})",
    "    return records",
    "",
    "# Paths",
    paste0("dbfile = r'", xml_file, "'"),
    paste0("dbout = r'", duckdb_file, "'"),
    "",
    "# Connect to DuckDB",
    "con = duckdb.connect(dbout)",
    "con.execute('CREATE TABLE IF NOT EXISTS drugbank_long (drug_id TEXT, key TEXT, value TEXT)')",
    "",
    "# Batch variables",
    paste0("batch_size = ", batchsize),
    "batch = []",
    "count = 0",
    "",
    "# Stream parse XML",
    "context = ET.iterparse(dbfile, events=('end',))",
    "for event, elem in context:",
    "    tag = strip_ns(elem.tag)",
    "    if tag == 'drug':",
    "        drug_id_elem = elem.find('.//{http://www.drugbank.ca}drugbank-id')",
    "        if drug_id_elem is None or not drug_id_elem.text:",
    "            elem.clear()",
    "            continue",
    "        drug_id = drug_id_elem.text",
    "        records = flatten_xml_long(elem, drug_id=drug_id)",
    "        if records:",
    "            batch.extend(records)",
    "        count += 1",
    "        # Insert batch",
    "        if count % batch_size == 0 and batch:",
    "            df = pd.DataFrame(batch)",
    "            con.execute('INSERT INTO drugbank_long SELECT * FROM df')",
    "            batch = []",
    "        if count % 1000 == 0:",
    "            print(f'Processed {count} drugs')",
    "        elem.clear()",
    "",
    "# Insert remaining batch",
    "if batch:",
    "    df = pd.DataFrame(batch)",
    "    con.execute('INSERT INTO drugbank_long SELECT * FROM df')",
    "",
    "con.close()",
    "print(f'Total real drugs inserted: {count}')"
  ), collapse = "\n")
  
  # Run the Python code
  reticulate::py_run_string(pystring)

  # Deduplicate the database
  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE), duckdb_file)
  if(!"drugbank" %in% dbListTables(con)){
    drugbank_dedup <- tbl(con, "drugbank_long") |> distinct() |>
    compute(name = "drugbank_long_dedup", temporary = FALSE)

    dbExecute(con,"DROP TABLE drugbank_long;")
    dbExecute(con,"ALTER TABLE drugbank_long_dedup RENAME TO drugbank;")
  }
  dbDisconnect(con)
}
