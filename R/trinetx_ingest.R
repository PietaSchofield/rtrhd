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

  # deps
  stopifnot(file.exists(dict_xlsx), dir.exists(data_dir))
  reqp <- c("DBI","duckdb","readxl")
  miss <- reqp[!vapply(reqp, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) stop("Missing packages: ", paste(miss, collapse=", "), call. = FALSE)

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
  `%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a

  build_schema <- function(df) {
    cols_needed <- c("Field name","Type","Format")
    if (!all(cols_needed %in% names(df)))
      stop("Sheet must contain columns: ", paste(cols_needed, collapse=", "), call. = FALSE)
    fields <- norm(df[["Field name"]])
    types  <- mapply(map_duck_type, df[["Type"]], df[["Format"]], USE.NAMES = FALSE)
    ddl    <- paste0(fields, " ", types, collapse = ", ")
    list(fields = fields, types = types, ddl = ddl)
  }

  # connect
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  DBI::dbExecute(con, sprintf("PRAGMA threads=%d;",(parallel::detectCores()-2)))
  # (optional) more throughput on big CSVs:
  DBI::dbExecute(con, "PRAGMA memory_limit='32GB';")  # adjust to your RAM
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE),
              silent = TRUE), add = TRUE)

  all_sheets <- readxl::excel_sheets(dict_xlsx)
  stem <- function(x) sub("\\.(csv|txt)$","",x,ignore.case=TRUE)
  all_stems <- stem(all_sheets)

  results <- list()

  # pre-list files once (case-insensitive match)
  all_files <- list.files(data_dir,
                          pattern = paste0("\\.", filetype, "(\\.gz)?$"),
                          full.names = TRUE, recursive = TRUE)
  base_lower <- tolower(basename(all_files))

  if(is.null(tables)){
    sheets <- all_sheets
  }else{
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

    sc <- try(build_schema(df), silent = TRUE)
    if (inherits(sc, "try-error")) {
      cat("  ! skipped (bad schema)\n")
      next
    }

    # skip if already in DB and not overwriting
    if (!overwrite && DBI::dbExistsTable(con, tname)) {
      cat("  ✔ already exists, skipping\n")
      next
    }

    # find file
    want <- tolower(c(paste0(sh_stem, ".", filetype),
                      paste0(sh_stem, ".", filetype, ".gz")))
    idx <- which(base_lower %in% want)
    if (!length(idx)) {
      cat("  ! no file found\n")
      next
    }
    fpath <- all_files[idx[1]]

    # (re)create table
    if (overwrite && DBI::dbExistsTable(con, tname))
      DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ",
                  DBI::dbQuoteIdentifier(con, tname)))
      DBI::dbExecute(con, paste0("CREATE TABLE IF NOT EXISTS ", 
                  DBI::dbQuoteIdentifier(con, tname), " (", sc$ddl, ");"))

    # read header
    peek_sql <- paste0(
      "SELECT * FROM read_csv_auto('", gsub("'", "''", fpath), "'",
      ", header=true, sample_size=1000, normalize_names=true",
      if (filetype == "txt") ", delim='\\t'" else "",
      ") LIMIT 0"
    )
    file_cols <- names(DBI::dbGetQuery(con, peek_sql))

    # build field list (using the improved strptime handling)
    fields <- sc$fields; types <- sc$types; fmts <- trimws(df$Format)
    present <- fields %in% file_cols
    sel_parts <- mapply(function(f, t, fmt, pres) {
      if (!pres) return(sprintf("CAST(NULL AS %s) AS %s", t, f))
      if (t == "TIMESTAMP") {
        fmt <- if (is.na(fmt) || fmt == "") "%Y-%m-%d %H:%M:%S" else fmt
        return(sprintf("strptime(CAST(%s AS VARCHAR), '%s') AS %s", f, fmt, f))
      }
      if (t == "DATE") {
        fmt <- if (is.na(fmt) || fmt == "") "%Y-%m-%d" else fmt
        return(sprintf("CAST(strptime(CAST(%s AS VARCHAR), '%s') AS DATE) AS %s", f, fmt, f))
      }
      sprintf("CAST(%s AS %s) AS %s", f, t, f)
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
  }

  cat("\nAll done in", round(difftime(Sys.time(), start_time, units = "mins"), 1), "minutes\n")

  invisible(results)
}
