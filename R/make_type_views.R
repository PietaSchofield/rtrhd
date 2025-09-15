#' Create typed DuckDB views from an Excel data dictionary (one sheet per table)
#'
#' Assumes one Excel sheet per table, with the sheet name matching the DuckDB
#' table name. The sheet must contain at least two columns: a variable name
#' column and a data type column. Raw tables remain all-VARCHAR; this builds
#' `<table>__typed` views that CAST matching columns.
#'
#' Name matching is tolerant:
#' - names are lowercased, trimmed, and non-alphanumerics collapsed to `_`
#' - exact normalised matches are cast; others stay as text
#'
#' @param con A \link[DBI]{DBIConnection} to DuckDB.
#' @param datadict_path Path to the Excel workbook (e.g., "gold_v26.xlsx").
#' @param name_col Column name in each sheet containing variable names.
#'   Default \code{"name"}.
#' @param type_col Column name in each sheet containing desired types.
#'   Default \code{"type"}.
#' @param only_tables Optional character vector of table names; if supplied,
#'   only those sheets/tables are processed.
#'
#' @return Invisibly, a named list. For each table, a list with
#'   \code{$matched} and \code{$unmatched} data frames describing mapping.
#' @examples
#' \dontrun{
#' make_typed_views(
#'   con           = dbcon,
#'   datadict_path = "gold_v26.xlsx",
#'   name_col      = "name",
#'   type_col      = "type"
#' )
#' }
#' @export
make_typed_views <- function(
  con,
  datadict_path,
  name_col   = "name",
  type_col   = "type",
  only_tables = NULL
) {
  stopifnot(file.exists(datadict_path))
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBI connection to DuckDB.")
  if (!requireNamespace("readxl", quietly = TRUE)) stop("Install 'readxl'")

  norm_name <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x)
    x <- gsub("_+", "_", x)
    x <- gsub("^_|_$", "", x)
    x
  }

  map_type <- function(t) {
    z <- tolower(trimws(as.character(t)))
    if (z %in% c("int", "integer", "int32"))                return("INTEGER")
    if (z %in% c("bigint", "long", "int64"))                return("BIGINT")
    if (z %in% c("smallint", "int16"))                      return("SMALLINT")
    if (z %in% c("tinyint", "byte", "int8"))                return("TINYINT")
    if (z %in% c("float", "double", "real", "numeric"))     return("DOUBLE")
    if (z %in% c("decimal", "number"))                      return("DECIMAL")   # width/scale unspecified
    if (z %in% c("bool", "boolean", "logical"))             return("BOOLEAN")
    if (z %in% c("date"))                                   return("DATE")
    if (z %in% c("datetime", "timestamp", "posixct"))       return("TIMESTAMP")
    if (z %in% c("time"))                                   return("TIME")
    if (z %in% c("text", "string", "varchar", "char"))      return("VARCHAR")
    "VARCHAR"
  }

  # discover tables present in DuckDB (to avoid building views for non-existent sheets/tables)
  db_tables <- DBI::dbGetQuery(con, "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main' AND table_type = 'BASE TABLE';
  ")$table_name

  sheets <- readxl::excel_sheets(datadict_path)
  if (!is.null(only_tables)) {
    sheets <- intersect(sheets, only_tables)
  }

  results <- list()
  # normalise function already exists inside, so reuse it
  db_map <- setNames(db_tables, norm_name(db_tables))

  for (sheet in sheets) {

    key <- norm_name(sheet)   # lower-cased, safe version
    if (!(key %in% names(db_map))) {
      message(sprintf("[skip] No table found for sheet '%s' (norm='%s')", sheet, key))
      next
    }
    tbl <- db_map[[key]]
    message("Processing sheet '", sheet, "' -> table '", tbl, "'")


    dd <- readxl::read_excel(datadict_path, sheet = sheet)
    if (!all(c(name_col, type_col) %in% names(dd))) {
      message(sprintf("[skip] Sheet '%s' missing columns '%s'/'%s'.",
                      sheet, name_col, type_col))
      next
    }

    dd$var_norm  <- norm_name(dd[[name_col]])
    dd$type_duck <- vapply(dd[[type_col]], map_type, character(1))

    # DB columns for this table
    cols <- DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s);",
                                         as.character(DBI::dbQuoteIdentifier(con, tbl))))
    if (!nrow(cols)) {
      message(sprintf("[skip] Table '%s' has no columns?", tbl))
      next
    }
    cols$norm <- norm_name(cols$name)

    # join by normalised name
    m <- merge(
      cols[, c("name", "norm")],
      dd[,   c("var_norm", "type_duck")],
      by.x = "norm", by.y = "var_norm",
      all.x = TRUE, sort = FALSE
    )

    # build SELECT list; cast only when a dict type is present
    select_items <- vapply(seq_len(nrow(m)), function(i) {
      col_id <- DBI::dbQuoteIdentifier(con, m$name[i])
      if (is.na(m$type_duck[i])) {
        sprintf("%s AS %s", col_id, col_id)
      } else {
        sprintf("CAST(%s AS %s) AS %s", col_id, m$type_duck[i], col_id)
      }
    }, character(1))

    view_name <- paste0(tbl, "__typed")
    sql <- sprintf(
      "CREATE OR REPLACE VIEW %s AS SELECT %s FROM %s;",
      as.character(DBI::dbQuoteIdentifier(con, view_name)),
      paste(select_items, collapse = ", "),
      as.character(DBI::dbQuoteIdentifier(con, tbl))
    )
    DBI::dbExecute(con, sql)

    unmatched <- subset(m, is.na(type_duck))[, c("name", "norm")]
    matched   <- subset(m, !is.na(type_duck))[, c("name", "norm")]
    results[[tbl]] <- list(matched = matched, unmatched = unmatched)

    if (nrow(unmatched)) {
      message(sprintf("[%s] view created; %d columns left as VARCHAR (no dict type): %s",
                      tbl, nrow(unmatched), paste(head(unmatched$name, 10), collapse = ", ")))
    } else {
      message(sprintf("[%s] view created; all columns typed.", tbl))
    }
  }

  invisible(results)
}
