#' Load lookup .txt files into DuckDB (folder-based rules)
#'
#' Recursively walks \code{lookups_dir}. For every \code{.txt} file:
#' \itemize{
#'   \item If the file is under a directory named \code{TXTFILES} (case-insensitive),
#'         its rows are appended to a consolidated table \code{lu_txtfiles(key, code, type,
#'         description)}.  Here, \code{key} is the filename (no extension, lower/safe), \code{code} is
#'         the first column value, \code{type} is the header name of the second column, and
#'         \code{description} is the second column value.  \item Otherwise, a table named
#'         \code{lu_<filename>} (lower/safe) is created/replaced and the file is
#'         bulk-loaded with DuckDB \code{COPY}. All columns are \code{VARCHAR}.
#' }
#'
#' @param lookups_dir Root directory containing lookup files.
#' @param con A DBI DBIConnection to DuckDB.
#' @param txt_glob Glob for lookup files. Default \code{"*.txt"}.
#' @param recreate_non_txtfiles If \code{TRUE}, drop & recreate \code{lu_<filename>} tables. 
#'        Default \code{TRUE}.
#'
#' @return Invisibly, a list with components \code{created_tables} and \code{txtfiles_rows}.
#' @export
gold_load_lookup_txt <- function(
  lookups_dir,
  con,
  txt_glob = "*.txt",
  recreate_non_txtfiles = TRUE
) {
  stopifnot(dir.exists(lookups_dir))
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBI connection to DuckDB.")

  norm <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x)
    x <- gsub("_+", "_", x)
    gsub("^_|_$", "", x)
  }
  qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

  # list all .txt files recursively
  all_dirs <- list.dirs(lookups_dir, recursive = TRUE, full.names = TRUE)
  all_paths <- unlist(lapply(c(lookups_dir, all_dirs), 
                             function(d) Sys.glob(file.path(d, txt_glob))), use.names = FALSE)
  all_paths <- unique(all_paths[file.exists(all_paths)])

  if (!length(all_paths)) {
    message("No .txt files found under: ", lookups_dir)
    return(invisible(list(created_tables = character(0), txtfiles_rows = 0L)))
  }

  # Prepare lu_txtfiles table (create if absent)
  if (!DBI::dbExistsTable(con, "lu_txtfiles")) {
    DBI::dbExecute(con, paste0("CREATE TABLE lu_txtfiles ",
                               "(key VARCHAR, code VARCHAR, type VARCHAR, description VARCHAR);"))
  }

  created <- character(0)
  txtfiles_n <- 0L

  for (f in all_paths) {
    rel <- sub(paste0("^", gsub("([\\^\\$\\.\\|\\(\\)\\[\\]\\*\\+\\?\\\\])", "\\\\\\1",
                                normalizePath(lookups_dir, winslash="/")),"/?"), "", 
               normalizePath(f, winslash="/"))
    path_parts <- strsplit(rel, "/")[[1]]

    # Is this under a TXTFILES directory (any level), case-insensitive?
    is_txtfiles <- any(tolower(path_parts) == "txtfiles")

    if (is_txtfiles) {
      # Consolidate into lu_txtfiles
      key <- norm(tools::file_path_sans_ext(basename(f)))

      # Read header to get 'type' = second column name
      header_line <- tryCatch(readLines(f, n = 1L, warn = FALSE), error = function(e) "")
      hdr <- if (length(header_line)) strsplit(header_line, "\t", fixed = TRUE)[[1]] else character(0)
      type_name <- if (length(hdr) >= 2) hdr[2] else "value"

      # Read file as character columns using base R (avoid extra deps)
      df <- tryCatch(utils::read.delim(f, sep = "\t", header = TRUE, stringsAsFactors = FALSE, 
                                       colClasses = "character"), error = function(e) NULL)
      if (is.null(df) || ncol(df) < 2) {
        message("[skip] TXTFILES '", rel, "' does not have at least two columns.")
        next
      }

      ins <- data.frame(
        key         = rep(key, nrow(df)),
        code        = df[[1]],
        type        = rep(type_name, nrow(df)),
        description = df[[2]],
        stringsAsFactors = FALSE
      )

      DBI::dbWriteTable(con, "lu_txtfiles", ins, append = TRUE)
      txtfiles_n <- txtfiles_n + nrow(ins)
    } else {
      # Create/replace lu_<filename> and COPY file into it
      base <- norm(tools::file_path_sans_ext(basename(f)))
      tbl <- paste0("lu_", base)

      # Read header to get column names
      header_line <- tryCatch(readLines(f, n = 1L, warn = FALSE), error = function(e) "")
      hdr <- if (length(header_line)) strsplit(header_line, "\t", fixed = TRUE)[[1]] else character(0)
      if (!length(hdr)) {
        message("[skip] '", rel, "' missing header; cannot create lu table.")
        next
      }

      # Create table with all VARCHAR columns (recreate if asked)
      if (DBI::dbExistsTable(con, tbl) && recreate_non_txtfiles) {
        DBI::dbRemoveTable(con, tbl)
      }
      if (!DBI::dbExistsTable(con, tbl)) {
        cols_sql <- paste(sprintf("%s VARCHAR", qid(norm(hdr))), collapse = ", ")
        DBI::dbExecute(con, sprintf("CREATE TABLE %s (%s);", qid(tbl), cols_sql))
      }

      # Bulk load via COPY (HEADER TRUE)
      copy_sql <- sprintf(
        paste(
          "COPY %s FROM %s (",
          "FORMAT 'csv', DELIMITER '\\t', HEADER true,",
          "QUOTE '', NULL '', SAMPLE_SIZE -1);"
        ),
        qid(tbl),
        DBI::dbQuoteString(con, normalizePath(f, winslash = "/"))
      )
      DBI::dbExecute(con, copy_sql)
      created <- unique(c(created, tbl))
    }
  }

  invisible(list(created_tables = created, txtfiles_rows = txtfiles_n))
}
