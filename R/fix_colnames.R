#' Harmonise dict names to schema headers
#' @param dict_names character vector from your Excel dict
#' @param xml_path path to one representative schema XML
#' @return named character vector: dict_name -> matched schema header
#' @export
align_dict_to_schema <- function(dict_names, xml_path) {
  stopifnot(file.exists(xml_path))
  if (!requireNamespace("xml2", quietly = TRUE)) stop("Install 'xml2'")

  norm <- function(x) {
    x |>
      trimws() |>
      tolower() |>
      gsub("[^a-z0-9]+", "_", x = _) |>
      gsub("^_|_$", "", x = _) |>
      gsub("_+", "_", x = _)
  }

  x <- xml2::read_xml(xml_path)
  headers <- xml2::xml_text(xml2::xml_find_all(x, "//recordset/datacolumns/datacolumn/header"))
  # the schema gives canonical column names like 'patid','gender','yob',… (no types). 
  # :contentReference[oaicite:1]{index=1}
  h_norm <- norm(headers)
  names(headers) <- h_norm

  d_norm <- norm(dict_names)

  # exact norm match first; fall back to closest (agrep) if needed
  out <- character(length(d_norm))
  for (i in seq_along(d_norm)) {
    dn <- d_norm[i]
    if (dn %in% h_norm) {
      out[i] <- headers[dn]
    } else {
      cand <- agrep(dn, h_norm, max.distance = 0.2, value = TRUE)
      out[i] <- if (length(cand)) headers[cand[[1]]] else NA_character_
    }
  }
  names(out) <- dict_names
  out
}
