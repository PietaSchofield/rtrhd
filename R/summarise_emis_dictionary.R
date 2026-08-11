#' Summarise SNOMED concepts with all semantic ancestors
#'
#' Produces word-level semantic summaries for all concepts, including ancestor semantics.
#'
#' @param db_path Path to DuckDB database
#' @param concept_table Name of concept table (default "terminology_concept")
#' @param description_table Name of description table (default "terminology_description")
#' @param relationship_table Name of relationship table (default "terminology_relationship")
#' @param stopwords Optional character vector of stopwords (default = tidytext::get_stopwords()$word)
#' @param workers Number of R cores to use for parallel processing (default 20)
#' @param threads Number of DuckDB threads for SQL query (default 20)
#'
#' @return A tibble with columns: conceptId, ancestorId, semantic, words (cleaned summary)
#' @export
summarise_snomed_ancestors <- function(
  db_path,
  concept_table = "terminology_concept",
  description_table = "terminology_description",
  relationship_table = "terminology_relationship",
  stopwords = tidytext::get_stopwords()$word,
  workers = 18,
  threads = 18
) {

  singularize_simple <- function(words) {
    words %>%
      stringr::str_replace_all("ies$", "y") %>%
      stringr::str_replace_all("(?<!s)s$", "")
  }
  if(F){
    db_path=adb
    threads=18
    concept_table = "terminology_concept"
    description_table = "terminology_description"
    relationship_table = "terminology_relationship"
  }

  # ---- open DuckDB with threads ----
  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE), dbdir = db_path, threads = threads)

  # ---- recursive query for all ancestors ----
  query <- glue::glue("
    WITH RECURSIVE ancestry AS (
      SELECT 
        r.sourceId AS conceptId,
        r.destinationId AS ancestorId
      FROM {relationship_table} r
      WHERE r.typeId = '116680003'  -- 'is a'
        AND r.active = '1'

      UNION ALL

      SELECT 
        a.conceptId,
        r.destinationId
      FROM {relationship_table} r
      JOIN ancestry a ON r.sourceId = a.ancestorId
      WHERE r.typeId = '116680003'
        AND r.active = '1'
    )
    SELECT DISTINCT
      a.conceptId,
      a.ancestorId,
      d.term AS ancestor_FSN
    FROM ancestry a
    LEFT JOIN {description_table} d
      ON a.ancestorId = d.conceptId
      AND d.typeId = '900000000000003001'
      AND d.active = '1'
  ")

  ancestry_table <- DBI::dbGetQuery(con, query)

  DBI::dbDisconnect(con)

  # ---- extract semantic and description ----
  ancestry_table <- ancestry_table %>%
    dplyr::mutate(
      description = stringr::str_remove(ancestor_FSN, " \\(.*\\)$"),
      semantic = stringr::str_extract(ancestor_FSN, "\\(.*\\)$") %>% stringr::str_remove_all("\\(|\\)"),
      words_list = stringr::str_to_lower(description) %>%
        stringr::str_replace_all("[^a-z ]", "") %>%
        stringr::str_split(" ") %>%
        purrr::map(~ purrr::discard(.x, ~ .x %in% stopwords)) %>%
        purrr::map(singularize_simple)
    )

  # ---- parallel collapse ----
  future::plan(future::multisession, workers = workers)

  ancestry_table <- ancestry_table %>%
    dplyr::mutate(
      words = furrr::future_map_chr(words_list, ~ paste(unique(.x), collapse = " "))
    ) %>%
    dplyr::select(conceptId, ancestorId, semantic, words)

  future::plan(future::sequential)

  return(ancestry_table)
}
