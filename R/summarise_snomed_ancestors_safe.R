#' Summarise SNOMED Ancestor Semantics (Final Semantic from FSN, Words from PT)
#'
#' Computes word-level semantic summaries for all SNOMED concepts and their
#' transitive ancestors. For each ancestor, the shortest FSN is used to extract
#' the final semantic (last parentheses), and words are taken from the PT.
#'
#' @param db_path Character. Path to the DuckDB database file.
#' @param concept_table Character. Name of the concept table (default: "terminology_concept").
#' @param description_table Character. Name of the description table (default: "terminology_description").
#' @param relationship_table Character. Name of the relationship table (default: "terminology_relationship").
#' @param language_refset_table Character. Name of the language refset table (default: "refset_language").
#' @param stopwords Character vector of stopwords to remove (default: tidytext::get_stopwords()$word).
#' @param threads Integer. Number of DuckDB threads (default: 8).
#' @param max_anc Maximum number of ancestors to follow (default: 2).
#' @param full_trace Boolean. Include full trace of ancestry (default: FALSE).
#' @param lang_refset_id Language refset ID for PT selection (default: 999000691000001104).
#'
#' @return A tibble with columns: conceptId, ancestorId, semantic, words
#' @export
summarise_snomed_ancestors_safe <- function(
  db_path,
  concept_table = "luemis_medicaldictionary",
  description_table = "terminology_description",
  relationship_table = "terminology_relationship",
  language_refset_table = "refset_language",
  stopwords = tidytext::get_stopwords()$word,
  rtypes = "'116680003','363398007','116676008'",
  threads = 8,
  max_anc = 2,
  full_trace = FALSE,
  lang_refset_id = '999000691000001104'
) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, threads = threads)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Copy stopwords into DuckDB
  stop_tbl <- tibble(word = stopwords)
  dplyr::copy_to(con, stop_tbl, name = "stopwords_tmp", temporary = TRUE, overwrite = TRUE)

  # Original recursive ancestry SQL
  ancestry_sql <- glue::glue("
    WITH RECURSIVE base AS (
      SELECT DISTINCT CAST(snomedctconceptid AS VARCHAR) AS conceptId
      FROM {concept_table}
      WHERE snomedctconceptid IS NOT NULL
    ),
    ancestry AS (
      SELECT conceptId, conceptId AS ancestorId, 0 AS ancno
      FROM base
      UNION ALL
      SELECT a.conceptId, r.destinationId AS ancestorId, a.ancno + 1 AS ancno
      FROM ancestry a
      JOIN {relationship_table} r
        ON r.sourceId = a.ancestorId
      WHERE r.active='1'
        AND r.typeId IN ({rtypes})
        AND r.destinationId != a.ancestorId
        AND a.ancno < {max_anc}
    ),

    ancestor_fsns AS (
      SELECT
        a.conceptId,
        a.ancestorId,
        d.term AS fsn_term,
        a.ancno,
        -- Extract final semantic tag
        CASE
          WHEN strpos(reverse(d.term), '(') > 0
          THEN rtrim(
                reverse(
                  substr(
                    reverse(d.term),
                    1,
                    strpos(reverse(d.term), '(') - 1
                  )
                ),
                ' )'
              )
          ELSE NULL
        END AS semantic,

        ROW_NUMBER() OVER (
          PARTITION BY a.conceptId, a.ancestorId,
                      CASE
                        WHEN strpos(reverse(d.term), '(') > 0
                        THEN rtrim(
                                reverse(
                                  substr(
                                    reverse(d.term),
                                    1,
                                    strpos(reverse(d.term), '(') - 1
                                  )
                                ),
                                ' )'
                              )
                        ELSE NULL
                      END
          ORDER BY length(d.term) ASC
        ) AS rn

      FROM (
        SELECT DISTINCT conceptId, ancestorId, ancno FROM ancestry
      ) a

      LEFT JOIN {description_table} d
        ON a.ancestorId = d.conceptId
        AND d.typeId == '900000000000003001'
        AND d.active = '1'
      WHERE
        a.ancestorID != '138875005'
    )

    SELECT conceptId, ancestorId, ancno, fsn_term, semantic
    FROM ancestor_fsns
    WHERE rn = 1
  
  ")

  fsn_tbl <- dplyr::tbl(con, dbplyr::sql(ancestry_sql))

  # PT via refset_language (your hard-won linkage)
  pt_tbl <-  dplyr::tbl(con, "terminology_description") |>
    select(id,pt_term=term,conceptId,typeId) |>
    filter(typeId != '900000000000003001') |>
    inner_join(
      dplyr::tbl(con, language_refset_table) |>
        filter(active=='1', acceptabilityId=='900000000000548007'),
      by= c("id" = "referencedComponentId")
    ) |>
    distinct(conceptId, pt_term)

  # Join ancestry → FSN & PT
  ancestry_desc <- fsn_tbl |>
    left_join(pt_tbl, by=c("ancestorId"="conceptId")) 

  # Tokenization: PT preferred, fallback FSN
  ancestry_desc <- ancestry_desc |>
    mutate(
      token_source = dbplyr::sql("coalesce(pt_term, fsn_term)"),
      token_source_clean = dbplyr::sql("trim(regexp_replace(token_source,' \\(.*\\)$',''))")
    )

  tokens <- ancestry_desc |>
    mutate(words = dbplyr::sql("unnest(regexp_extract_all(lower(token_source_clean),'[a-z]+'))")) |>
    filter(words != "") |>
    anti_join(dplyr::tbl(con,"stopwords_tmp"), by=c("words"="word")) |>
    mutate(words = dbplyr::sql("
      CASE
        WHEN words LIKE '%ies' THEN regexp_replace(words,'ies$','y')
        WHEN words LIKE '%s' AND words NOT LIKE '%ss' THEN regexp_replace(words,'s$','')
        ELSE words
      END
    "))

  # Aggregate
  gbcols <- if(full_trace) c("conceptId","ancestorId","ancno","fsn_term","pt_term","semantic") else c("conceptId","semantic")

  result <- tokens |>
    group_by(across(all_of(gbcols))) |>
    summarise(words = dbplyr::sql("string_agg(DISTINCT words,' ')"), .groups="drop") |>
    collect()

  return(result)
}
