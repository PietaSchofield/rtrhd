#' Load lookup tables from GOLD XML schemas into DuckDB
#'
#' Walks subdirectories of \code{root_dir}, reads one XML schema per directory,
#' and for each \code{<recordset>} and \code{<datacolumn>} that contains
#' \code{<lookups>}, creates a DuckDB table named
#' \code{<table>__lk_<column>} with columns \code{code} and \code{label}.
#' Also maintains a consolidated \code{gold_lookup_catalog}
#' with \code{table_name}, \code{column_name}, \code{code}, \code{label}.
#'
#' Table/column names are normalised to lower case with underscores.
#' Existing lookup tables of the same name are replaced.
#'
#' @param root_dir Root directory containing subfolders with one schema XML.
#' @param con DBI connection to DuckDB.
#' @param xml_glob Glob for schema files within each folder. Default \code{"*.xml"}.
#' @return Invisibly, a data.frame summary of lookups created.
#' @examples
#' \dontrun{
#' gold_load_lookups(golddir, dbcon)
#' }
#' @export
gold_load_lookups <- function(root_dir, con, xml_glob = "*.xml") {
  stopifnot(dir.exists(root_dir))
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBI connection to DuckDB.")
  if (!requireNamespace("xml2", quietly = TRUE)) stop("Install 'xml2'")

  norm <- function(x) {
    x <- trimws(tolower(as.character(x)))
    x <- gsub("[^a-z0-9]+", "_", x)
    x <- gsub("_+", "_", x)
    gsub("^_|_$", "", x)
  }
  qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

  dirs <- list.dirs(root_dir, full.names = TRUE, recursive = FALSE)
  if (!length(dirs)) dirs <- root_dir

  catalog <- list()

  for (d in dirs) {
    xmls <- Sys.glob(file.path(d, xml_glob))
    if (!length(xmls)) next
    x <- xml2::read_xml(xmls[[1]])

    # each recordset = a logical table (e.g., "Patient")
    rsets <- xml2::xml_find_all(x, "//recordset")
    for (rs in rsets) {
      tbl_raw <- xml2::xml_attr(rs, "name")
      if (is.na(tbl_raw) || tbl_raw == "") next
      tbl <- norm(tbl_raw)

      dcols <- xml2::xml_find_all(rs, ".//datacolumn")
      for (dc in dcols) {
        col_hdr <- xml2::xml_text(xml2::xml_find_first(dc, "header"))
        if (is.na(col_hdr) || col_hdr == "") next
        col <- norm(col_hdr)

        lookups <- xml2::xml_find_all(dc, ".//lookups/lookup")
        if (!length(lookups)) next

        codes  <- xml2::xml_attr(lookups, "id")
        labels <- xml2::xml_text(lookups)

        lk_df <- data.frame(
          code  = as.character(codes),
          label = as.character(labels),
          stringsAsFactors = FALSE
        )

        # write per-column lookup table
        lk_table <- paste0(tbl, "__lk_", col)
        if (DBI::dbExistsTable(con, lk_table)) {
          DBI::dbRemoveTable(con, lk_table)
        }
        DBI::dbWriteTable(con, lk_table, lk_df)

        # add to catalog
        catalog[[length(catalog) + 1L]] <- transform(
          lk_df,
          table_name  = tbl,
          column_name = col
        )
      }
    }
  }

  if (length(catalog)) {
    cat_df <- do.call(rbind, catalog)[, c("table_name","column_name","code","label")]
    # replace consolidated catalog
    if (DBI::dbExistsTable(con, "gold_lookup_catalog")) {
      DBI::dbRemoveTable(con, "gold_lookup_catalog")
    }
    DBI::dbWriteTable(con, "gold_lookup_catalog", cat_df)
  } else {
    cat_df <- data.frame(table_name=character(), column_name=character(),
                         code=character(), label=character(), stringsAsFactors=FALSE)
  }

  invisible(cat_df)
}
