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
                                      reppref = FALSE, ow = FALSE, db = FALSE,
                                      filetype=c("txt","csv")) {
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
      filetype = filetype,
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
construct_col_types <- function(data_dict,
                                ddcols = c("name","description","type","format"),
                                setcols = TRUE) {
  if (setcols) {
    names(data_dict) <- c("name","description","type","format")
  } else {
    data_dict <- dplyr::select(data_dict, dplyr::any_of(ddcols))
  }

  norm <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x)
    gsub("_+", "_", x)
  }

  data_dict$name <- norm(data_dict$name)
  data_dict$type <- toupper(data_dict$type)

  # map dict → readr types (keep huge IDs as text)
  type_mapping <- list(
    "CHAR"     = readr::col_character(),
    "TEXT"     = readr::col_character(),
    "VARCHAR"  = readr::col_character(),
    "NUMERIC"  = readr::col_double(),
    "DECIMAL"  = readr::col_double(),
    "INTEGER"  = readr::col_integer(),
    "BIGINT"   = readr::col_character(),  # <- avoid overflow; treat as text
    "LOGICAL"  = readr::col_logical(),
    "BOOLEAN"  = readr::col_logical(),
    "DATE"     = function(fmt) readr::col_date(format = fmt),
    "DATETIME" = function(fmt) readr::col_datetime(format = fmt)
  )

  col_types <- list()
  for (i in seq_len(nrow(data_dict))) {
    nm  <- data_dict$name[i]
    typ <- data_dict$type[i]
    fmt <- if (!is.null(data_dict$format)) data_dict$format[i] else NA

    if (typ %in% c("DATE","DATETIME") && !is.na(fmt) && nzchar(fmt)) {
      col_types[[nm]] <- type_mapping[[typ]](fmt)
    } else if (!is.null(type_mapping[[typ]])) {
      col_types[[nm]] <- type_mapping[[typ]]
    } else {
      # default: keep as character so nothing is lost
      col_types[[nm]] <- readr::col_character()
    }
  }

  readr::cols(.default = readr::col_skip(), !!!col_types)
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
                                           reppref = FALSE, ow = FALSE,
                                           db = FALSE,filetype=c("txt","csv"),
                                           log_to_duckdb = TRUE,
                                           log_table = "load_problems") {
  filetype <- match.arg(filetype)
  delim <- if(identical(filetype,"csv")) "," else "\t"

  norm <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x); gsub("_+", "_", x)
  }

  # table name
  fname0 <- gsub("[.].*", "", fname)
  tname <- if (reppref) tolower(paste0(dset, "_", gsub("^[^_]*_", "", fname0)))
           else         tolower(paste0(dset, "_", fname0))

  # get existing tables
  dbc_tmp <- duckdb::dbConnect(duckdb::duckdb(), dbf, write = FALSE)
  tabs <- DBI::dbListTables(dbc_tmp)
  DBI::dbDisconnect(dbc_tmp)

  if (!tname %in% tabs || ow) {
    if (ow) {
      rtrhd::sql_execute(dbf = dbf, sql_make = paste0("DROP TABLE IF EXISTS ", tname, ";"))
    }

    # build col_types (case-insensitive on names)
    col_types <- rtrhd::construct_col_types(ddict)

    # discover files
    all_files <- list.files(path = ddir, pattern = paste0(".*", fname0), full.names = TRUE,
                            recursive = TRUE)
    pattern <- rtrhd::construct_pattern(stub = fname0, protocol = proto,filetype=filetype)
    fpath <- all_files[grepl(pattern, basename(all_fi/les), perl = TRUE)]
    if (!length(fpath)) {
      logger::log_warn("No files matched pattern {pattern} for {fname0}")
      return(NULL)
    }

    # prepare problem log (DuckDB or CSV)
    append_problem_rows <- function(df) {
      if (is.null(df) || !nrow(df)) return(invisible())
      if (isTRUE(log_to_duckdb)) {
        conl <- duckdb::dbConnect(duckdb::duckdb(), dbf, write = TRUE)
        if (!DBI::dbExistsTable(conl, log_table)) {
          DBI::dbExecute(conl, sprintf(
            "CREATE TABLE %s (when_ts TIMESTAMP, table_name VARCHAR, file VARCHAR,
                              row INTEGER, col INTEGER, col_name VARCHAR,
                              expected VARCHAR, actual VARCHAR, problem VARCHAR);",
            DBI::dbQuoteIdentifier(conl, log_table)
          ))
        }
        DBI::dbWriteTable(conl, log_table, df, append = TRUE)
        DBI::dbDisconnect(conl)
      }
      invisible()
    }

    # load files one by one with detailed warning/error capture
    totals <- integer(0)
    for (fn in fpath) {
      warn_msgs <- character(0)
      prob_rows <- NULL

      # capture warnings without losing detail
      dat <- withCallingHandlers(
        tryCatch(
          readr::read_delim(
            file = fn,
            delim = delim,
            col_types = col_types,
            progress = FALSE,
            show_col_types = FALSE,
            locale = readr::locale(encoding = "UTF-8", decimal_mark = ".")
          ),
          error = function(e) {
            # record as a "problem" row with problem text
            prob_rows <<- rbind(
              prob_rows,
              data.frame(
                when_ts = Sys.time(),
                table_name = tname,
                file = fn,
                row = NA_integer_,
                col = NA_integer_,
                col_name = NA_character_,
                expected = NA_character_,
                actual = NA_character_,
                problem = paste("ERROR:", conditionMessage(e)),
                stringsAsFactors = FALSE
              )
            )
            return(NULL)
          }
        ),
        warning = function(w) {
          warn_msgs <<- c(warn_msgs, conditionMessage(w))
          invokeRestart("muffleWarning")
        }
      )

      # collect readr parse problems (row/col level)
      if (!is.null(dat)) {
        p <- readr::problems(dat)

        if (nrow(p)) {
          # normalise across readr versions
          if (!"file" %in% names(p)) p$file <- fn
          # derive col_name and problem for our log schema
          p$col_name <- if ("col" %in% names(p)) as.character(p$col) else NA_character_
          p$problem  <- paste0("expected=", p$expected, " | actual=", p$actual)

          p$when_ts    <- Sys.time()
          p$table_name <- tname

          p <- p[, c("when_ts","table_name","file","row","col","col_name","expected","actual","problem")]

          prob_rows <- rbind(prob_rows, p)
        }
      }

      # append any high-level warnings as problem rows
      if (length(warn_msgs)) {
        prob_rows <- rbind(
          prob_rows,
          data.frame(
            when_ts = Sys.time(),
            table_name = tname,
            file = fn,
            row = NA_integer_,
            col = NA_integer_,
            col_name = NA_character_,
            expected = NA_character_,
            actual = NA_character_,
            problem = paste(warn_msgs, collapse = " | "),
            stringsAsFactors = FALSE
          )
        )
      }

      # print a concise on-screen diagnostic if requested
      if (isTRUE(db) && !is.null(prob_rows) && nrow(prob_rows)) {
        cat("\n--- parse issues ---\n")
        print(utils::head(prob_rows, 10))
        if (nrow(prob_rows) > 10) cat(sprintf("... and %d more\n", nrow(prob_rows) - 10))
      }

      # log to DuckDB / CSV
      append_problem_rows(prob_rows)

      # rename cols (lowercase, safe) after reading
      if (!is.null(dat) && nrow(dat)) {
        names(dat) <- norm(names(dat))
        conw <- duckdb::dbConnect(duckdb::duckdb(), dbf, write = TRUE)
        DBI::dbWriteTable(conw, tname, dat, append = TRUE, overwrite = FALSE)
        DBI::dbDisconnect(conw)
        totals <- c(totals, nrow(dat))
      } else {
        totals <- c(totals, 0L)
      }
    }

    logger::log_info("{tname}: {sum(totals)} records loaded from {length(fpath)} file(s)")
    invisible(totals)
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
construct_pattern <- function(stub, protocol, filetype=c("txt","csv")) {
  filetype <- match.arg(filetype)
  ext <- paste0("\\.", filetype, "$")

  if (is.null(protocol)&identical(filetype,"csv")) {
    # exact filename match: stub.<ext>
    paste0("^", stub, ext)
  } else if (is.null(protocol)){
    paste0("*_",stub,"_.*",ext)
  } else if (grepl("_pathway$", stub)) {
    # stub(_YYYY)?_protocol.<ext>
    paste0("^", stub, "(_\\d{4})?_", protocol, ext)
  } else {
    # stub but not *_pathway, optional _YYYY, then _protocol.<ext>
    paste0("^", stub, "(?!_pathway)(_\\d{4})?_", protocol, ext)
  }
}

