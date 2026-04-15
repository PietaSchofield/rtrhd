#' Parse DrugBank XML to DuckDB (long format, batch inserts)
#'
#' @param xml_file Path to full_database.xml
#' @param duckdb_file Path to output .duckdb file
#' @param batchsize Number of drugs to batch insert (default 1000)
#' @param overwrite Overwrite existing DuckDB file (default FALSE)
#' @export
drugbank_xml2duckdb_2 <- function(xml_file, duckdb_file, batchsize = 1000, overwrite = FALSE) {

  if (file.exists(duckdb_file) && !overwrite) {
    message("DuckDB file already exists, skipping.")
    return(invisible(NULL))
  }

  reticulate::py_require(c("duckdb"))
  unlink(duckdb_file)

  pystring <- paste(c(
"import xml.etree.ElementTree as ET",
"import duckdb",

"def strip_ns(tag):",
"    return tag.split('}', 1)[1] if '}' in tag else tag",

paste0("xml_file = r'", xml_file, "'"),
paste0("db_file = r'", duckdb_file, "'"),

"con = duckdb.connect(db_file)",

"# -------- Tables --------",
"con.execute('''",
"CREATE TABLE drugbank_drugs (",
"  drug_id TEXT,",
"  name TEXT,",
"  type TEXT",
")''')",

"con.execute('''",
"CREATE TABLE external_identifiers (",
"  drug_id TEXT,",
"  resource TEXT,",
"  identifier TEXT",
")''')",

"con.execute('''",
"CREATE TABLE atc_codes (",
"  drug_id TEXT,",
"  atc_code TEXT,",
"  level1 TEXT,",
"  level2 TEXT,",
"  level3 TEXT,",
"  level4 TEXT,",
"  level5 TEXT",
")''')",

"con.execute('''",
"CREATE TABLE drug_synonyms (",
"  drug_id TEXT,",
"  name TEXT,",
"  type TEXT",
")''')",

paste0("batch_size = ", batchsize),

"drug_rows = []",
"ext_rows = []",
"atc_rows = []",
"syn_rows = []",

"count = 0",

"context = ET.iterparse(xml_file, events=('end',))",

"for event, elem in context:",
"    tag = strip_ns(elem.tag)",

"    if tag == 'drug':",

"        drug_id_elem = elem.find('.//{http://www.drugbank.ca}drugbank-id')",
"        name_elem = elem.find('.//{http://www.drugbank.ca}name')",

"        if drug_id_elem is None or not drug_id_elem.text:",
"            elem.clear()",
"            continue",

"        drug_id = drug_id_elem.text",
"        name = name_elem.text if name_elem is not None else None",
"        dtype = elem.attrib.get('type', None)",

"        # -------- Core drug --------",
"        drug_rows.append((drug_id, name, dtype))",

"        if name:",
"            syn_rows.append((drug_id, name, 'primary'))",

"        # -------- Synonyms --------",
"        syns = elem.findall('.//{http://www.drugbank.ca}synonym')",

"        for s in syns:",
"            if s.text:",
"                syn_rows.append((drug_id, s.text, 'synonym'))",

"        # -------- External IDs --------",
"        ext_ids = elem.findall('.//{http://www.drugbank.ca}external-identifier')",

"        for ext in ext_ids:",
"            resource = ext.find('{http://www.drugbank.ca}resource')",
"            identifier = ext.find('{http://www.drugbank.ca}identifier')",

"            if resource is not None and identifier is not None:",
"                ext_rows.append((drug_id, resource.text, identifier.text))",

"        # -------- ATC --------",
"        atcs = elem.findall('.//{http://www.drugbank.ca}atc-code')",

"        for atc in atcs:",
"            code = atc.attrib.get('code', None)",

"            levels = {'level1': None, 'level2': None, 'level3': None, 'level4': None, 'level5': None}",

"            for level in atc.findall('{http://www.drugbank.ca}level'):",
"                lvl_code = level.attrib.get('code', None)",
"                lvl_name = level.text",

"                if lvl_code:",
"                    if len(lvl_code) == 1:",
"                        levels['level1'] = lvl_name",
"                    elif len(lvl_code) == 3:",
"                        levels['level2'] = lvl_name",
"                    elif len(lvl_code) == 4:",
"                        levels['level3'] = lvl_name",
"                    elif len(lvl_code) == 5:",
"                        levels['level4'] = lvl_name",
"                    elif len(lvl_code) == 7:",
"                        levels['level5'] = lvl_name",

"            atc_rows.append((",
"                drug_id, code,",
"                levels['level1'],",
"                levels['level2'],",
"                levels['level3'],",
"                levels['level4'],",
"                levels['level5']",
"            ))",

"        count += 1",

"        # -------- Batch insert --------",
"        if count % batch_size == 0:",

"            if drug_rows:",
"                con.executemany('INSERT INTO drugbank_drugs VALUES (?, ?, ?)', drug_rows)",
"                drug_rows = []",

"            if syn_rows:",
"                con.executemany('INSERT INTO drug_synonyms VALUES (?, ?, ?)', syn_rows)",
"                syn_rows = []",

"            if ext_rows:",
"                con.executemany('INSERT INTO external_identifiers VALUES (?, ?, ?)', ext_rows)",
"                ext_rows = []",

"            if atc_rows:",
"                con.executemany('INSERT INTO atc_codes VALUES (?, ?, ?, ?, ?, ?, ?)', atc_rows)",
"                atc_rows = []",

"        if count % 1000 == 0:",
"            print(f'Processed {count} drugs')",

"        elem.clear()",

"# -------- Final flush --------",
"if drug_rows:",
"    con.executemany('INSERT INTO drugbank_drugs VALUES (?, ?, ?)', drug_rows)",

"if syn_rows:",
"    con.executemany('INSERT INTO drug_synonyms VALUES (?, ?, ?)', syn_rows)",

"if ext_rows:",
"    con.executemany('INSERT INTO external_identifiers VALUES (?, ?, ?)', ext_rows)",

"if atc_rows:",
"    con.executemany('INSERT INTO atc_codes VALUES (?, ?, ?, ?, ?, ?, ?)', atc_rows)",

"con.close()",
"print(f'Total drugs processed: {count}')"

  ), collapse = "\n")

  reticulate::py_run_string(pystring)

  message("DrugBank DuckDB created successfully.")
}
