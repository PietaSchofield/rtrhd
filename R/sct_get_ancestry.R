#' Get SNOMED hierarchy for a concept (ancestors / descendants)
#'
#' Walks the SNOMED CT *is-a* graph stored in DuckDB and returns the concept
#' itself (`depth = 0`) plus ancestors (`direction = "up"`), descendants
#' (`"down"`), or both (`"both"`).
#'
#' @param dbf      DuckDB **connection** created earlier.
#' @param concept_id Character SNOMED concept ID to trace from.
#' @param direction "up", "down", or "both" (default).
#' @param rel_table  Name of relationship table (default `"terminology_relationship"`).
#' @param desc_table Name of description  table (default `"terminology_description"`).
#'
#' @return A tibble: conceptId, term, depth (0 = self), path ("up"/"down").
#' @export
get_snomed_hierarchy <- function(dbf,
                                 concept_id,
                                 direction = c("up", "down", "both"),
                                 rel_table  = "terminology_relationship",
                                 desc_table = "terminology_description",
                                 counts = F) {

  direction <- match.arg(direction)
  res_list  <- list()          # collect result sets

  ## --------------------  DOWN (children, grandchildren, …) --------------------
  if (direction %in% c("down", "both")) {

    sql_down <- glue::glue("
      WITH RECURSIVE down_hierarchy(conceptId, depth) AS (
        SELECT '{concept_id}', 0
        UNION ALL
        SELECT r.sourceId, dh.depth - 1
        FROM down_hierarchy dh
        JOIN {rel_table} r
          ON r.destinationId = dh.conceptId
        WHERE r.active = 1
          AND r.typeId  = '116680003'          -- is-a
      )
      SELECT dh.conceptId,
             td.term,
             dh.depth,
             'down' AS path
      FROM down_hierarchy dh
      LEFT JOIN {desc_table} td
        ON td.conceptId = dh.conceptId
      WHERE td.active = 1
        AND td.typeId = '900000000000003001'   -- FSN
    ")

    res_list$down <- rtrhd::get_table(dbf = dbf, sqlstr = sql_down)
  }

  ## --------------------  UP (parents, grandparents, …) ------------------------
  if (direction %in% c("up", "both")) {

    sql_up <- glue::glue("
      WITH RECURSIVE up_hierarchy(conceptId, depth) AS (
        SELECT '{concept_id}', 0
        UNION ALL
        SELECT r.destinationId, uh.depth + 1
        FROM up_hierarchy uh
        JOIN {rel_table} r
          ON r.sourceId = uh.conceptId
        WHERE r.active = 1
          AND r.typeId  = '116680003'          -- is-a
      )
      SELECT uh.conceptId,
             td.term,
             uh.depth,
             'up' AS path
      FROM up_hierarchy uh
      LEFT JOIN {desc_table} td
        ON td.conceptId = uh.conceptId
      WHERE td.active = 1
        AND td.typeId = '900000000000003001'   -- FSN
    ")

    res_list$up <- rtrhd::get_table(dbf = dbf, sqlstr = sql_up)
  }

  if(counts){
    lapply(res_list,function(rl) dplyr::distinct(rl) %>% nrow()) %>% plyr::ldply()
  }else{
    dplyr::bind_rows(res_list) %>% dplyr::select(-path) %>% dplyr::distinct()
  }
}

