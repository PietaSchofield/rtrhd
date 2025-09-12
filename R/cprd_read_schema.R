#' Read CPRD extract schema (XML) into a tidy map
#'
#' Parses a CPRD `*_Extract_schema.xml` file and returns a named list.
#' Each list element corresponds to a `<recordset>` and contains a tibble
#' with the exact column headers and their 1-based positions in-file.
#'
#' @param schema_path Character scalar. Path to the CPRD XML schema file
#'   (e.g., `"path/to/goldcases_Extract_schema.xml"`).
#'
#' @return A named list. Names are recordset names from the schema.
#'   Each element is a tibble with columns:
#'   \describe{
#'     \item{header}{Character; exact column header as it appears in data files.}
#'     \item{pos}{Integer; 1-based column position in the file.}
#'   }
#'   If the file is missing or empty, returns an empty list.
#'
#' @examples
#' \dontrun{
#'   smap <- read_cprd_schema(".../goldcases_Extract_schema.xml")
#'   names(smap)              # recordset names (e.g., "Therapy", "Test")
#'   smap$Therapy             # tibble of headers and positions
#' }
#'
#' @export
read_cprd_schema <- function(schema_path) {
  stopifnot(length(schema_path) == 1L, is.character(schema_path))
  if (!file.exists(schema_path)) return(structure(list(), names = character()))

  x <- xml2::read_xml(schema_path)

  recs <- xml2::xml_find_all(x, ".//recordset")
  if (length(recs) == 0L) return(structure(list(), names = character()))

  rec_names <- xml2::xml_attr(recs, "name")

  out <- purrr::map(recs, function(rec) {
    headers <- xml2::xml_text(xml2::xml_find_all(rec, ".//datacolumn/header"))
    pos0    <- xml2::xml_text(xml2::xml_find_all(rec, ".//datacolumn/columnposition"))
    pos1    <- suppressWarnings(as.integer(pos0) + 1L)

    tibble::tibble(
      header = headers,
      pos    = pos1
    ) |>
      dplyr::arrange(.data$pos)
  })

  stats::setNames(out, rec_names)
}
