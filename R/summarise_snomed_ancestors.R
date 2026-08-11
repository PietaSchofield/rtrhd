#' Summarise SNOMED Ancestor Semantics (Final Semantic, Shortest FSN, Clean Words)
#'
#' Computes word-level semantic summaries for all SNOMED concepts and their
#' transitive ancestors. For each ancestor and semantic tag, the shortest
#' Fully Specified Name (FSN) is selected. Only the final semantic (last parentheses)
#' is captured, ensuring accurate abstraction for higher-level summaries.
#'
#' @param db_path Character. Path to the DuckDB database file.
#' @param concept_table Character. Name of the concept table (default: "terminology_concept").
#' @param description_table Character. Name of the description table (default: "terminology_description").
#' @param relationship_table Character. Name of the relationship table (default: "terminology_relationship").
#' @param stopwords Character vector of stopwords to remove (default: tidytext::get_stopwords()$word).
#' @param threads Integer. Number of DuckDB threads (default: 8).
#' @param max_anc set the maximum number of ancestors to consider
#'
#' @return A tibble with columns: conceptId, ancestorId, semantic, words
#' @export
summarise_snomed_ancestors_old <- function(
  db_path,
  concept_table = "terminology_concept",
  description_table = "terminology_description",
  relationship_table = "terminology_relationship",
  stopwords = tidytext::get_stopwords()$word,
  rtypes = "'116680003','363398007','116676008'",
  ctypes = "'900000000000509007', '900000000000003001'",
  threads = 8,
  max_anc = 2,
  full_trace = FALSE
) {

  # Connect to DuckDB
  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE), dbdir = db_path, threads = threads)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Copy stopwords to DuckDB
  stop_tbl <- tibble::tibble(word = stopwords)
  dplyr::copy_to(con, stop_tbl, name = "stopwords_tmp", temporary = TRUE, overwrite = TRUE)

  # Recursive ancestry + shortest FSN per final semantic
  ancestry_sql <- glue::glue("
    WITH RECURSIVE base AS (
      -- 🔑 Seed from EMIS dictionary
      SELECT DISTINCT 
        CAST(snomedctconceptid AS VARCHAR) AS conceptId
      FROM luemis_medicaldictionary
      WHERE snomedctconceptid IS NOT NULL
    ),

    ancestry AS (
      -- ✅ Include self to guarantee coverage
      SELECT 
        conceptId,
        conceptId AS ancestorId,
        0 AS ancno
      FROM base

      UNION ALL

      -- ✅ Traverse SNOMED IS-A hierarchy
      SELECT 
        a.conceptId,
        r.destinationId AS ancestorId,
        a.ancno + 1 AS ancno
      FROM ancestry a
      JOIN {relationship_table} r
        ON r.sourceId = a.ancestorId
      WHERE r.active = '1'
        AND r.typeId IN ({rtypes})
        AND r.destinationId != a.ancestorId
        AND a.ancno < {max_anc}
    ),

    ancestor_fsns AS (
      SELECT
        a.conceptId,
        a.ancestorId,
        d.term AS ancestor_term,
        d.typeId AS ancestor_typeId,  
        CASE 
          WHEN d.typeId = '900000000000003001' THEN 'FSN'
          WHEN d.typeId = '900000000000509007' THEN 'PT'
          ELSE 'OTHER'
        END AS term_type,
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
          PARTITION BY a.conceptId, a.ancestorId, d.typeId,
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
        AND d.typeId IN ({ctypes})
        AND d.active = '1'
      WHERE
        a.ancestorID != '138875005'
    )

    SELECT conceptId, ancestorId, ancno, ancestor_term, term_type, semantic
    FROM ancestor_fsns
    WHERE rn = 1
  ")

  ancestry <- dplyr::tbl(con, dbplyr::sql(ancestry_sql))

  # Tokenise description into alphabetic sequences only
  tokens <- ancestry |>
    dplyr::mutate(
      word = dbplyr::sql("
        unnest(
          regexp_extract_all(
            lower(trim(regexp_replace(ancestor_term, ' \\(.*\\)$', ''))),
            '[a-z]+'
          )
        )
      ")
    ) |>
    dplyr::filter(word != "")

  # Remove stopwords
  tokens <- tokens |>
    dplyr::anti_join(dplyr::tbl(con, "stopwords_tmp"), by = "word")

  # Simple singularisation (basic rules)
  tokens <- tokens |>
    dplyr::mutate(
      word = dbplyr::sql("
        CASE 
          WHEN word LIKE '%ies' THEN regexp_replace(word, 'ies$', 'y')
          WHEN word LIKE '%s' AND word NOT LIKE '%ss' THEN regexp_replace(word, 's$', '')
          ELSE word
        END
      ")
    )

  if(full_trace){
    gbcode <- c("conceptId","ancestorId","ancno","ancestor_term","term_type","semantic")
  }else{
    gbcode <- c("conceptId","semantic")
  }

  # Aggregate unique words per concept-ancestor-semantic
  result <- tokens |>
    dplyr::group_by(dplyr::across(dplyr::all_of(gbcode))) |>
    dplyr::summarise(
      words = dbplyr::sql("string_agg(DISTINCT word, ' ')"),
      .groups = "drop"
    ) |>
    dplyr::collect()

  return(result)
}
