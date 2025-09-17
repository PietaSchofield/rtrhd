#' Import SNOMED CT release files into a DuckDB database
#'
#' Scans a folder of SNOMED CT `.txt` files and loads terminology and refset tables into DuckDB.
#'
#' @param dbPath Path to folder where the DuckDB file is or will be created.
#' @param txtPath Path to SNOMED CT release folder.
#' @param dbName Optional filename for DuckDB (default: based on `txtPath`).
#' @param db Internal testing flag to override paths. Ignored in normal use.
#' @param ow Overwrite existing tables? Default is FALSE (safer).
#' @param preterm Pattern to remove from terminology file names.
#' @param posterm Pattern to remove from terminology file names.
#' @param prerefs Pattern to remove from refset file names.
#' @param posrefs Pattern to remove from refset file names.
#' @param incterm Optional character vector of terminology tables to include (e.g. "concept").
#' @param increfs Optional character vector of refset tables to include (e.g. "language").
#'
#' @return Path to the DuckDB file.
#' @importFrom magrittr %>%
#' @export
sct_make_db <- function(dbPath, txtPath, dbName = NULL, db = FALSE, ow = FALSE,
                        preterm = "sct2_", posterm = "_MONOSnap.*",
                        prerefs = ".*efset_", posrefs = "MONOSnap.*",
                        incterm = NULL, increfs = NULL) {

  if (is.null(dbName)) {
    dbFile <- file.path(dbPath, paste0("sct_", basename(txtPath), ".duckdb"))
  } else {
    dbFile <- file.path(dbPath, dbName)
  }

  ## Load terminology files
  term_files <- list.files(txtPath, pattern = "sct.*txt", recursive = TRUE, full.names = TRUE)
  if (length(term_files) == 0) stop("No terminology files found in: ", txtPath)

  names(term_files) <- gsub(paste0("(", preterm, "|", posterm, ")"), "", basename(term_files))
  names(term_files) <- gsub(paste0("(", prerefs, "|", posrefs, ")"), "", names(term_files)) %>%
    tolower() %>%
    make.names(unique = TRUE)
  names(term_files) <- paste0("terminology_", gsub("\\.", "_", names(term_files)))

  message("Terminology tables discovered: ", paste(names(term_files), collapse = ", "))

  if (!is.null(incterm)) {
    term_files <- term_files[gsub("^terminology_", "", names(term_files)) %in% incterm]
    if (length(term_files) == 0) 
      stop("No matching terminology tables found for: ", paste(incterm, collapse = ", "))
  }

  lapply(names(term_files), load_sct_file, dbf = dbFile, filelist = term_files, ow = ow)

  ## Load refset files
  ref_files <- list.files(txtPath, pattern = "der.*txt", recursive = TRUE, full.names = TRUE)
  if (length(ref_files) == 0) stop("No refset files found in: ", txtPath)

  names(ref_files) <- gsub(paste0("(", prerefs, "|", posrefs, ")"), "", basename(ref_files)) %>%
    tolower() %>%
    make.names(unique = TRUE)
  names(ref_files) <- paste0("refset_", gsub("\\.", "_", names(ref_files)))

  message("Refset tables discovered: ", paste(names(ref_files), collapse = ", "))

  if (!is.null(increfs)) {
    ref_files <- ref_files[gsub("^refset_", "", names(ref_files)) %in% increfs]
    if (length(ref_files) == 0) 
      stop("No matching refset tables found for: ", paste(increfs, collapse = ", "))
  }

  lapply(names(ref_files), load_sct_file, dbf = dbFile, filelist = ref_files, ow = ow)

  return(dbFile)
}

#' Load a single SNOMED CT file into DuckDB
#'
#' Loads one file into a DuckDB table, skipping or overwriting as specified.
#'
#' @param tab Table name.
#' @param dbf Path to DuckDB file.
#' @param filelist Named list of filenames.
#' @param ow Overwrite if table exists? (default FALSE)
#'
#' @return NULL (invisible), or result from load_table
#' @export
load_sct_file <- function(tab, dbf, filelist, ow = FALSE) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbf)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  if (tab %in% DBI::dbListTables(con)) {
    if (!ow) {
      message("Skipping existing table: ", tab)
      return(invisible(NULL))
    } else {
      warning("Overwriting existing table: ", tab)
    }
  }

  rtrhd::load_table(
    filename = filelist[[tab]],
    dbf = dbf,
    tab_name = tab,
    ow = ow
  )
}

