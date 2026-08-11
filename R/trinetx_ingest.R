#' Ingest TriNetX tables into DuckDB from an Excel data dictionary
#'
#' Reads an Excel data dictionary with one sheet per table (columns: "Field name", "Type", "Format"),
#' creates DuckDB tables, and ingests data files that exactly match each sheet name
#' (e.g. sheet "patient.csv" -> file "patient.csv", case-insensitive; also supports ".csv.gz").
#'
#' @param dict_xlsx Path to the Excel data dictionary.
#' @param data_dir  Directory containing the data files.
#' @param db_path   Path to the DuckDB database file.
#' @param filetype  One of "csv" or "txt".
#' @param overwrite If TRUE, drops existing tables before load.
#' @return Invisibly, a named list of rows loaded per table.
#' @export
trinetx_ingest <- function(dict_xlsx,
                           data_dir,
                           db_path,
                           filetype = c("csv","txt"),
                           overwrite = TRUE,
                           tables = NULL) {
  filetype <- match.arg(filetype)

  stopifnot(file.exists(dict_xlsx), dir.exists(data_dir))
  reqp <- c("DBI","duckdb","readxl")
  miss <- reqp[!vapply(reqp, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) stop("Missing packages: ", paste(miss, collapse=", "), call. = FALSE)

  `%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a

  norm <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x); gsub("_+", "_", x)
  }
  map_duck_type <- function(typ, fmt) {
    t <- toupper(trimws(typ %||% ""))
    if (t %in% c("CHAR","TEXT","VARCHAR")) return("VARCHAR")
    if (t %in% c("INTEGER","INT"))         return("INTEGER")
    if (t == "BIGINT")                     return("BIGINT")
    if (t %in% c("NUMERIC","DECIMAL","DOUBLE","FLOAT","REAL")) return("DOUBLE")
    if (t %in% c("BOOLEAN","LOGICAL"))     return("BOOLEAN")
    if (t == "DATE")                       return("DATE")
    if (t == "DATETIME")                   return("TIMESTAMP")
    "VARCHAR"
  }

  # NOTE now takes `con` so it can quote identifiers consistently.
  # THIS quoting is the fix for the March "TYPE" field SNAFU.
  build_schema <- function(df, con) {
    cols_needed <- c("Field name","Type","Format")
    if (!all(cols_needed %in% names(df)))
      stop("Sheet must contain columns: ", paste(cols_needed, collapse=", "), call. = FALSE)
    fields <- norm(df[["Field name"]])
    types  <- mapply(map_duck_type, df[["Type"]], df[["Format"]], USE.NAMES = FALSE)
    quoted_fields <- vapply(fields, function(f) as.character(DBI::dbQuoteIdentifier(con, f)),
                             character(1))
    ddl <- paste0(quoted_fields, " ", types, collapse = ", ")
    list(fields = fields, types = types, ddl = ddl)
  }

  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE), db_path)
  DBI::dbExecute(con, sprintf("PRAGMA threads=%d;", max(1, parallel::detectCores() - 2)))
  DBI::dbExecute(con, "PRAGMA memory_limit='32GB';")
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

  all_sheets <- readxl::excel_sheets(dict_xlsx)
  stem <- function(x) sub("\\.(csv|txt)$","",x,ignore.case=TRUE)
  all_stems <- stem(all_sheets)

  results <- list()

  all_files <- list.files(data_dir,
                          pattern = paste0("\\.", filetype, "(\\.gz)?$"),
                          full.names = TRUE, recursive = TRUE)
  base_lower <- tolower(basename(all_files))

  if (is.null(tables)) {
    sheets <- all_sheets
  } else {
    want <- tolower(stem(tables))
    keep <- tolower(all_stems) %in% want
    sheets <- all_sheets[keep]
  }

  n_total <- length(sheets)
  start_time <- Sys.time()

  for (i in seq_along(sheets)) {
    sh <- sheets[i]
    sh_stem <- sub("\\.(csv|txt)$", "", sh, ignore.case = TRUE)
    tname   <- norm(sh_stem)

    cat(sprintf("\n[%02d/%02d] %s -> %s ...\n", i, n_total, sh, tname))

    df <- readxl::read_excel(dict_xlsx, sheet = sh)
    names(df) <- trimws(names(df))

    sc <- try(build_schema(df, con), silent = TRUE)
    if (inherits(sc, "try-error")) {
      cat("  ! skipped (bad schema)\n")
      next
    }

    if (!overwrite && DBI::dbExistsTable(con, tname)) {
      cat("  ✔ already exists, skipping\n")
      next
    }

    want <- tolower(c(paste0(sh_stem, ".", filetype),
                      paste0(sh_stem, ".", filetype, ".gz")))
    idx <- which(base_lower %in% want)
    if (!length(idx)) {
      cat("  ! no file found\n")
      next
    }
    fpath <- all_files[idx[1]]

    if (overwrite && DBI::dbExistsTable(con, tname)) {
      DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ",
                  DBI::dbQuoteIdentifier(con, tname)))
    }
    DBI::dbExecute(con, paste0("CREATE TABLE IF NOT EXISTS ",
                DBI::dbQuoteIdentifier(con, tname), " (", sc$ddl, ");"))

    peek_sql <- paste0(
      "SELECT * FROM read_csv_auto('", gsub("'", "''", fpath), "'",
      ", header=true, sample_size=1000, normalize_names=true",
      if (filetype == "txt") ", delim='\\t'" else "",
      ") LIMIT 0"
    )
    file_cols <- names(DBI::dbGetQuery(con, peek_sql))

    qid <- function(x) DBI::dbQuoteIdentifier(con, x)

    fields <- sc$fields
    types <- sc$types
    fmts <- trimws(df$Format)

    present <- fields %in% file_cols

    sel_parts <- mapply(function(f, t, fmt, pres) {
      f_q <- as.character(qid(f))

      if (!pres) return(sprintf("CAST(NULL AS %s) AS %s", t, f_q))

      if (t == "TIMESTAMP") {
        fmt <- if (is.na(fmt) || fmt == "") "%Y-%m-%d %H:%M:%S" else fmt
        return(sprintf("strptime(CAST(%s AS VARCHAR), '%s') AS %s", f_q, fmt, f_q))
      }

      if (t == "DATE") {
        fmt <- if (is.na(fmt) || fmt == "") "%Y-%m-%d" else fmt
        return(sprintf("CAST(strptime(CAST(%s AS VARCHAR), '%s') AS DATE) AS %s", f_q, fmt, f_q))
      }

      sprintf("CAST(%s AS %s) AS %s", f_q, t, f_q)   # <-- fixed: f_g -> f_q

    }, fields, types, fmts, present, USE.NAMES = FALSE)

    sql <- paste0(
      "INSERT INTO ", DBI::dbQuoteIdentifier(con, tname), " ",
      "SELECT ", paste(sel_parts, collapse = ", "),
      " FROM read_csv_auto('", gsub("'", "''", fpath), "'",
      ", header=true, sample_size=-1, normalize_names=true",
      if (filetype == "txt") ", delim='\\t'" else "",
      ");"
    )

    t0 <- Sys.time()
    DBI::dbExecute(con, sql)
    n <- DBI::dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM ",
              DBI::dbQuoteIdentifier(con, tname)))$n
    took <- round(difftime(Sys.time(), t0, units = "mins"), 1)
    cat(sprintf("  ✔ %s rows loaded (%.1f min)\n", format(n, big.mark=","), took))
    results[[tname]] <- n
  }

  cat("\nAll done in", round(difftime(Sys.time(), start_time, units = "mins"), 1), "minutes\n")

  invisible(results)
}
