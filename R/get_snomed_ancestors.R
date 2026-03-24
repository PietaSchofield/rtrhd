#' Get SNOMED Ancestors from a Database
#'
#' Retrieves SNOMED ancestors and optionally summarises terms by semantic type.
#'
#' @param concept_id SNOMED concept ID
#' @param db_path Path to DuckDB database
#' @param concept_table Name of concept table
#' @param relationship_table Name of relationship table
#' @param description_table Name of description table
#' @param summarise Logical; if TRUE return semantic summary
#'
#' @return A data frame of ancestors or a summarised version by semantic type
#' @export
get_snomed_ancestors <- function(
  concept_id,
  db_path,
  concept_table = "terminology_concept",
  relationship_table = "terminology_relationship",
  description_table = "terminology_description",
  summarise = FALSE
) {

  # ---- helper: simple plural normalisation ----
  singularize_simple <- function(words) {
    words %>%
      stringr::str_replace_all("ies$", "y") %>%
      stringr::str_replace_all("(?<!s)s$", "") %>%
      stringr::str_replace_all("(xes|ses|zes|ches|shes)$", ~ stringr::str_remove(.x, "es$"))
  }

  # ---- helper: semantic summarisation ----
  summarise_semantic <- function(df) {
    stop_words_vec <- tidytext::get_stopwords()$word

    df %>%
      dplyr::group_by(semantic) %>%
      dplyr::arrange(desc(nch), .by_group = TRUE) %>%
      dplyr::slice_head(n = 2) %>%
      dplyr::summarise(
        words = description %>%
          stringr::str_to_lower() %>%
          stringr::str_replace_all("[^a-z ]", "") %>%
          stringr::str_split(" ") %>%
          unlist() %>%
          purrr::discard(~ .x %in% stop_words_vec) %>%
          purrr::discard(~ .x == "") %>%
          singularize_simple() %>%
          unique() %>%
          paste(collapse = " "),
        .groups = "drop"
      )
  }

  # ---- DB query ----
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)

  query <- glue::glue("
  WITH RECURSIVE ancestry AS (
      SELECT 
          r.sourceId AS ancestor_id,
          c.id AS concept_id,
          r.typeId AS rtype_id,
          r.sourceId || ' -> ' || c.id AS path
      FROM {relationship_table} r
      JOIN {concept_table} c ON r.destinationId = c.id
      WHERE r.sourceId = ?
        AND r.typeId IN ('116680003', '363698007', '116676008')
        AND r.active = '1'
        AND r.characteristicTypeId IN ('900000000000011006', '900000000000010007')

      UNION ALL

      SELECT 
          r.sourceId AS ancestor_id,
          c.id AS concept_id,
          r.typeId AS rtype_id,
          a.path || ' -> ' || c.id AS path
      FROM {relationship_table} r
      JOIN ancestry a ON r.destinationId = a.ancestor_id
      JOIN {concept_table} c ON r.sourceId = c.id
      WHERE r.typeId = '116680003'
        AND r.active = '1'
        AND r.characteristicTypeId IN ('900000000000011006', '900000000000010007')
  )
  SELECT DISTINCT 
      adesc.term AS Source,
      ancestry.rtype_id,
      sdesc.term AS Ancestor
  FROM ancestry
  LEFT JOIN {description_table} sdesc ON
     ancestry.concept_id = sdesc.conceptId
     AND sdesc.typeId = '900000000000003001'
     AND sdesc.active = '1'
  LEFT JOIN {description_table} adesc ON 
     ancestry.ancestor_id = adesc.conceptId
     AND adesc.typeId = '900000000000003001'
     AND adesc.active = '1'
  ORDER BY path;
  ")

  result <- DBI::dbGetQuery(con, query, params = list(concept_id)) %>%
    dplyr::mutate(
      nch = nchar(Ancestor),
      description = stringr::str_remove(Ancestor, " \\(.*\\)$"),
      semantic = stringr::str_extract(Ancestor, "\\(.*\\)$") %>%
        stringr::str_remove_all("\\(|\\)")
    )

  DBI::dbDisconnect(con)

  # ---- optional summarisation ----
  if (summarise) {
    return(summarise_semantic(result))
  }

  return(result)
}
