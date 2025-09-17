#' Ingest a directory of ZIPs into DuckDB (low-memory, one ZIP at a time)
#'
#' Extracts each `.zip` to a temp folder, loads all tab-delimited `.txt` files
#' into a DuckDB database, then deletes the temp files. Tables are created or
#' appended based on the base `.txt` filename. Two special rules are baked in:
#' `include_Define_results.txt` → table `Patients`, and any `include_Define_log.txt`
#' is skipped. New columns found in later files are added to existing tables as
#' `TEXT` to avoid schema drift failures.
#'
#' @param dbfile   Character scalar. Path to the DuckDB database file to create/use.
#' @param zip_dir  Character scalar. Directory containing `.zip` files to ingest.
#' @param zip_glob Character scalar. Glob pattern for ZIP names within `zip_dir`.
#'   Default: `"*.zip"`.
#'
#' @details
#' **Workflow**
#' 1. Find ZIPs by `zip_glob` under `zip_dir`.
#' 2. For each ZIP: extract to a temporary directory, read every `*.txt` as TSV with
#'    all columns as character, write/append into DuckDB, then remove the temp dir.
#' 3. Table naming: base of the `.txt` after removing the `Inc[0-9]_` prefix and any
#'    trailing `_[0-9]+` or spaces; lowercased and non-alphanumerics converted to `_`.
#'
#' **Routing rules**
#' - Files named exactly `include_Define_results.txt` → table `Patients`.
#' - Files named exactly `include_Define_log.txt` are ignored.
#'
#' **Schema evolution**
#' - If a later file has extra columns, they are added to the DuckDB table as `TEXT`.
#' - Missing columns in an incoming file are filled with `NA_character_`.
#'
#' **Performance notes**
#' - Processes one ZIP at a time to keep RAM and disk I/O low.
#' - Reads with `colClasses = "character"` to avoid expensive type inference and
#'   heterogeneity across files.
#'
#' @return Invisibly returns `TRUE`. Prints a short per-table row-count summary.
#'
#' @examples
#' \dontrun{
#' ingest_zips_to_duckdb(
#'   dbfile  = "data.duckdb",
#'   zip_dir = file.path(.locData, "define")
#' )
#' }
#'
#' @importFrom DBI dbConnect dbDisconnect dbExecute dbExistsTable dbListFields
#'   dbQuoteIdentifier dbGetQuery
#' @import duckdb
#' @importFrom tools file_path_sans_ext
#' @importFrom utils unzip read.delim
#' @export
ingest_zips_to_duckdb <- function(dbfile, zip_dir, zip_glob = "*.zip") {
  stopifnot(dir.exists(zip_dir))
  zips <- Sys.glob(file.path(zip_dir, zip_glob))
  if (length(zips) == 0L) { message("No zips found."); return(invisible(TRUE)) }

  suppressPackageStartupMessages({
    library(DBI); library(duckdb); library(tools)
  })
  con <- dbConnect(duckdb(dbfile, read_only = FALSE))
  on.exit({ try(dbDisconnect(con, shutdown = TRUE), silent = TRUE) }, add = TRUE)

  sanitize <- function(x) tolower(gsub("[^A-Za-z0-9_]+", "_", x))

  add_missing_cols <- function(tbl, df) {
    existing <- tryCatch(DBI::dbListFields(con, tbl), error = function(e) character())
    if (length(existing) == 0L && !DBI::dbExistsTable(con, tbl)) {
      DBI::dbWriteTable(con, tbl, df, temporary = FALSE)
      return(invisible(TRUE))
    }
    extra_in_df <- setdiff(names(df), existing)
    if (length(extra_in_df)) {
      for (cn in extra_in_df) {
        DBI::dbExecute(con, sprintf(
          "ALTER TABLE %s ADD COLUMN %s TEXT;",
          DBI::dbQuoteIdentifier(con, tbl),
          DBI::dbQuoteIdentifier(con, cn)
        ))
      }
      existing <- DBI::dbListFields(con, tbl)
    }
    miss_in_df <- setdiff(existing, names(df))
    if (length(miss_in_df)) for (cn in miss_in_df) df[[cn]] <- NA_character_
    df <- df[, existing, drop = FALSE]
    DBI::dbWriteTable(con, tbl, df, append = TRUE)
    invisible(TRUE)
  }

  extract_with_cli <- function(zip, outdir) {
    # -o overwrite, -q quiet; returns exit status (0 = OK)
    status <- system2("unzip", c("-oq", shQuote(zip), "-d", shQuote(outdir)))
    if (is.na(status) || status != 0) {
      stop(sprintf("Failed to extract via system unzip: %s (status %s)", zip, status))
    }
    invisible(TRUE)
  }

  for (z in zips) {
    tmp <- tempfile("unz_"); dir.create(tmp)

    # use system unzip (avoids utils::unzip() spurious 'corrupt' warnings)
    extract_with_cli(z, tmp)

    txts <- list.files(tmp, pattern = "\\.txt$", recursive = TRUE, full.names = TRUE)
    if (!length(txts)) { unlink(tmp, recursive = TRUE); message("No .txt in ", basename(z)); next }

    for (p in txts) {
      fname <- basename(p)
      if (grepl("_Define_log\\.txt$", fname, ignore.case = TRUE)) next

      if (grepl("_Define_results\\.txt$", fname, ignore.case = TRUE)) {
        base <- "Patients"
      } else {
        stem <- tools::file_path_sans_ext(fname)
        # Capture: ...Inc<digits>_<label>(_<runno>)?
        # Keep "Inc<digits>_<label>", drop trailing _### if present
        captured <- sub(
          "^.*?(Inc[0-9]+)_([^_].*?)(?:_[0-9]+)?$",
          "\\1_\\2",
          stem,
          perl = TRUE
        )

        # Fallback if it didn’t match (keeps your old behaviour but names it clearly)
        if (identical(captured, stem)) {
          captured <- paste0(
            "uncategorised_",
            gsub("([ ]|_[0-9]*$)", "", gsub(".*Inc[0-9]_", "", stem))
          )
        }
        base <- captured
      }

      tbl <- sanitize(base)

      if (file.info(p)$size <= 0) next

      df <- utils::read.delim(
        p, header = TRUE, sep = "\t", quote = "",
        check.names = FALSE, colClasses = "character", comment.char = ""
      )

      if (!DBI::dbExistsTable(con, tbl)) DBI::dbWriteTable(con, tbl, df, temporary = FALSE)
      else add_missing_cols(tbl, df)
    }

    unlink(tmp, recursive = TRUE)
    message(sprintf("Processed: %s", basename(z)))
  }

  cats <- DBI::dbGetQuery(con, "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name;")
  for (t in cats$table_name) {
    n <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM %s;", DBI::dbQuoteIdentifier(con, t)))$n
    message(sprintf("Table %s: %,d rows", t, n))
  }

  invisible(TRUE)
}
