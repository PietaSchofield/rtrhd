#' Load All Files Specified in a Data Dictionary
#'
#' Iterates over each sheet in the Excel data dictionary and loads associated data files into
#' a database table using the column specifications in the sheet.
#'
#' @param ddFile Path to the Excel data dictionary file. Each sheet represents a table.
#' @param dbf Path to the DuckDB database file.
#' @param datadir Directory containing the data files to be loaded.
#' @param protocol Optional protocol string to match in filenames.
#' @param dset Dataset name prefix used in constructing table names.
#' @param reppref Logical; if TRUE, uses the portion after the first underscore in the filename as table suffix.
#' @param ow Logical; if TRUE, overwrite existing tables.
#' @param db Logical; if TRUE, prints problems encountered during data load.
#'
#' @return Invisibly returns a list of load results per sheet.
#' @export
load_with_data_dictionary <- function(ddFile, dbf, datadir, protocol = NULL, dset, 
                                      reppref = FALSE, ow = FALSE, db = FALSE) {
  sheetlist <- rtrhd::read_excel_sheets_to_list(ddFile)
  res <- lapply(names(sheetlist), function(fn) {
    rtrhd::load_file_with_data_dictionary(
      fname = fn,
      ddict = sheetlist[[fn]],
      dset = dset,
      reppref = reppref,
      ddir = datadir,
      dbf = dbf,
      proto = protocol,
      ow = ow,
      db = db
    )
  })
  invisible(res)  # Ensure result is returned, even if silently
}

#' Construct `col_types` for `readr::read_tsv` from a Data Dictionary
#'
#' Translates data dictionary types to `readr` `col_types` format for parsing text files.
#'
#' @param data_dict A data.frame with columns: name, description, type, and format.
#' @param ddcols a vector of column names in the data dictionary meta data
#' @param setcols A boolean TRUE to rename the column names or FALSE select existing columns
#'
#' @return A `readr::cols()` specification object.
#' @export
construct_col_types <- function(data_dict,ddcols=c("name","description","type","format"),setcols=T) {
  if(F){
    data_dict <- dd[[3]]
    ddcols=c("name","description","type","format")
    setcols=F
  }
  
  if(setcols){
    names(data_dict) <- c("name", "description", "type", "format")
  }else{
    data_dict <- data_dict %>% dplyr::select(dplyr::any_of(ddcols))
  }
  
  # Normalize type names to uppercase for case-insensitive matching
  data_dict$type <- toupper(data_dict$type)

  type_mapping <- list(
    "CHAR" = col_character(),
    "VARCHAR" = col_character(),
    "NUMERIC" = col_double(),
    "DECIMAL" = col_double(),
    "INTEGER" = col_integer(),
    "BIGINT" = col_character(),
    "LOGICAL" = col_logical(),
    "BOOLEAN" = col_logical(),
    "DATE" = function(format) col_date(format = format),
    "DATETIME" = function(format) col_datetime(format = format)
  )

  col_types <- list()
  for (i in seq_len(nrow(data_dict))) {
    col_name <- data_dict$name[i]
    col_type <- data_dict$type[i]
    col_format <- data_dict$format[i]

    if ((col_type %in% c("DATE", "DATETIME")) && !is.na(col_format)) {
      col_types[[col_name]] <- type_mapping[[col_type]](col_format)
    } else if (!is.null(type_mapping[[col_type]])) {
      col_types[[col_name]] <- type_mapping[[col_type]]
    }
  }
  cols(.default = col_skip(), !!!col_types)
}

#' Load a File Using Data Dictionary Information
#'
#' Searches for files that match a pattern derived from the sheet name, loads them with correct
#' column types, and inserts the data into a DuckDB database table.
#'
#' @param fname Filename base (typically from the sheet name).
#' @param ddict Data dictionary for the file.
#' @param ddir Directory to search for matching files.
#' @param dbf Path to the DuckDB database file.
#' @param proto Optional protocol to match in filenames.
#' @param dset Dataset name prefix for constructing the table name.
#' @param reppref Logical; use suffix from filename after first underscore.
#' @param ow Logical; overwrite the existing table if it exists.
#' @param db Logical; if TRUE, prints load issues to console.
#'
#' @return Number of rows loaded per file.
#' @export
load_file_with_data_dictionary <- function(fname, ddict, ddir, dbf, proto, dset, 
                                           reppref = FALSE, ow = FALSE, db = FALSE) {
  fname <- gsub("[.].*", "", fname)
  tname <- if (reppref) {
    tolower(paste0(dset, "_", gsub("^[^_]*_", "", fname)))
  } else {
    tolower(paste0(dset, "_", fname))
  }

  dbc <- duckdb::dbConnect(duckdb::duckdb(), dbf, write = FALSE)
  tabs <- DBI::dbListTables(dbc)
  duckdb::dbDisconnect(dbc, shutdown = TRUE)

  if (!tname %in% tabs || ow) {
    if (ow) {
      rtrhd::sql_execute(dbf = dbf, sql_make = paste0("DROP TABLE IF EXISTS ", tname, ";"))
    }

    col_types <- rtrhd::construct_col_types(data_dict = ddict)
    all_files <- list.files(path = ddir, pattern = paste0(".*", fname), full.names = TRUE, recursive = TRUE)
    pattern <- rtrhd::construct_pattern(stub = fname, protocol = proto)
    fpath <- all_files[grepl(pattern, basename(all_files), perl = TRUE)]

    if (length(fpath) == 0) {
      logger::log_warn("No files matched pattern {pattern} for {fname}")
      return(NULL)
    }

    res <- lapply(fpath, function(fn) {
      dat <- readr::read_tsv(fn, col_types = col_types)
      if (db) {
        cat(paste0(tname, ": ", paste0(readr::problems(dat), collapse = " , "), "\n"))
      }
      names(dat) <- tolower(gsub("[.]", "_", make.names(names(dat), unique = TRUE)))

      if (nrow(dat) > 0) {
        dbc <- duckdb::dbConnect(duckdb::duckdb(), dbf, write = TRUE)
        DBI::dbWriteTable(dbc, tname, dat, append = TRUE, overwrite = FALSE)
        duckdb::dbDisconnect(dbc, shutdown = TRUE)
      }
      nrow(dat)
    })
    logger::log_info("{tname}: {sum(unlist(res))} records loaded")
  }
}

#' Construct a Regex Pattern for Matching Data Files
#'
#' Builds a regular expression to match filenames based on a stub and optional protocol.
#'
#' @param stub Base name used to identify files.
#' @param protocol Optional protocol string to match.
#'
#' @return A character string containing a regular expression.
#' @export
construct_pattern <- function(stub, protocol) {
  if (is.null(protocol)) {
    paste0(".*_", stub, "_.*.txt$")
  } else {
    if (grepl("_pathway$", stub)) {
      paste0("^", stub, "(_\\d{4})?_", protocol, "\\.txt$")
    } else {
      # Assumes negative lookahead is supported
      paste0("^", stub, "(?!_pathway)(_\\d{4})?_", protocol, "\\.txt$")
    }
  }
}

