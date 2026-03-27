#' Classify a SNOMED term with weighted multi-output scoring
#'
#' Applies stub-to-system and stub-to-activity dictionaries with weights
#' to infer what a clinician might have been thinking for a given term.
#'
#' @param term Character string of a SNOMED CT term
#' @param thinking_df Data frame with columns: stub, system, weight
#' @param normalize Logical, whether to normalize scores to max 1 (default FALSE)
#'
#' @return A tibble with columns:
#'   - term: the original SNOMED term
#'   - system: candidate clinical system (multiple rows per term possible)
#'   - system_score: summed weight for that system
#'
#' @import stringr
#' @import dplyr
#'
#' @export
classify_words_weighted <- function(words, semantic, thinking_df, normalize = FALSE) {

  if(length(words) !=1 || is.na(words) || words == "") return(
    tibble::tibble(
      thinking = NA_character_,
      system_score = NA_real_,
      semantic = semantic
    )
  )
  
  words_lower <- str_to_lower(words)
  
  # --- SYSTEM MATCHES ---
  system_matches <- thinking_df |>
    mutate(match = str_detect(words_lower, stub)) |>
    filter(match) |>
    group_by(thinking) |>
    summarise(system_score = sum(weight), .groups = "drop") |>
    mutate(semantic = semantic)
  
  if (nrow(system_matches) == 0) {
    system_matches <- tibble(thinking = "Unclassified", semantic=semantic, system_score = 1)
  }
  
  # --- NORMALIZE SCORES ---
  if (normalize) {
    system_matches <- system_matches |> 
      dplyr::mutate(system_score = system_score / max(system_score))
  }
  
  return(system_matches)
}
