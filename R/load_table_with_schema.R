#' Import GOLD tab-delimited files into DuckDB using XML schema
#'
#' For each subdirectory under `root_dir`, this function looks for one XML schema
#' file (describing the columns) and one or more tab-delimited `.txt` files
#' (containing the data). It creates a DuckDB table named after the subdirectory
#' (lowercased) if it does not exist and appends rows from all `.txt` files.
#'
#' If `root_dir` itself contains XML/TXT files (no subdirectories), a single
#' table is created using the lowercased basename of `root_dir`.
#'
#' All columns are created as \code{VARCHAR} to avoid premature type coercion.
#' Any lookup information in the XML is ignored at import time and can be
#' handled later during analysis.
#'
#' @param root_dir Character scalar. Path to the root directory containing
#'   subdirectories with XML + TXT files, or containing files itself.
#' @param con A live \link[DBI]{DBIConnection} to a DuckDB database.
#' @param txt_glob Pattern for tab-delimited files. Default \code{"*.txt"}.
#' @param xml_glob Pattern for XML schema files. Default \code{"*.xml"}.
#' @param has_header Logical. If \code{TRUE}, the TXT files are assumed to
#'   contain a header row. Default \code{FALSE}.
#'
#' @return Invisibly returns a named integer vector: the number of TXT files
#'   successfully loaded into each table.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "gold.duckdb")
#' loaded <- gold_import(
#'   root_dir   = "/path/to/root/with/subdirs",
#'   con        = con,
#'   txt_glob   = "*.txt",
#'   xml_glob   = "*.xml",
#'   has_header = FALSE
#' )
#' loaded
#' }
#'
#' @export
gold_import <- function(
  root_dir,
  con,
  txt_glob = "*.txt",
  xml_glob = "*.xml",
  has_header = TRUE 
) {
  stopifnot(dir.exists(root_dir))
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBI connection to DuckDB.")

  # helpers
  quote_ident <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))
  create_table_if_absent <- function(tbl, cols) {
    if (!DBI::dbExistsTable(con, tbl)) {
      cols_sql <- paste(sprintf("%s VARCHAR", quote_ident(cols)), collapse = ", ")
      DBI::dbExecute(con, sprintf("CREATE TABLE %s (%s);", quote_ident(tbl), cols_sql))
    }
  }
  parse_schema_cols <- function(xml_path) {
    x <- xml2::read_xml(xml_path)
    nodes <- xml2::xml_find_all(x, "//recordset/datacolumns/datacolumn")
    ord  <- as.integer(xml2::xml_text(xml2::xml_find_all(nodes, "columnposition")))
    hdrs <- xml2::xml_text(xml2::xml_find_all(nodes, "header"))
    hdrs[order(ord)]
  }

  dirs <- list.dirs(root_dir, full.names = TRUE, recursive = FALSE)
  if (!length(dirs)) dirs <- root_dir

  results <- integer(0)

  for (d in dirs) {
    tbl_name <- tolower(basename(d))
    xmls <- Sys.glob(file.path(d, xml_glob))
    if (!length(xmls)) next
    schema_cols <- parse_schema_cols(xmls[[1]])

    create_table_if_absent(tbl_name, schema_cols)

    txts <- Sys.glob(file.path(d, txt_glob))
    if (!length(txts)) next

    count <- 0L
    for (f in txts) {
      copy_sql <- sprintf(
        paste(
          "COPY %s FROM %s (",
          "FORMAT 'csv', DELIMITER '\\t', HEADER %s,",
          "QUOTE '',  NULL '',",
          "SAMPLE_SIZE -1);"
        ),
        quote_ident(tbl_name),
        DBI::dbQuoteString(con, normalizePath(f, winslash = "/")),
        if (has_header) "true" else "false"
      )
      DBI::dbExecute(con, copy_sql)
      count <- count + 1L
    }
    results[tbl_name] <- count
  }
  invisible(results)
}
