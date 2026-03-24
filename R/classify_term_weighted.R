#' Classify a SNOMED term with weighted multi-output scoring
#'
#' Applies stub-to-system and stub-to-activity dictionaries with weights
#' to infer what a clinician might have been thinking for a given term.
#'
#' @param term Character string of a SNOMED CT term
#' @param thinking_df Data frame with columns: stub, system, weight
#' @param activity_df Data frame with columns: stub, activity, weight
#' @param normalize Logical, whether to normalize scores to max 1 (default FALSE)
#'
#' @return A tibble with columns:
#'   - term: the original SNOMED term
#'   - system: candidate clinical system (multiple rows per term possible)
#'   - system_score: summed weight for that system
#'   - activity: candidate activity type (multiple rows per term possible)
#'   - activity_score: summed weight for that activity
#'
#' @import stringr
#' @import dplyr
#'
#' @export
classify_term_weighted <- function(term, thinking_df, activity_df, normalize = FALSE) {

  if(is.na(term || term == "")) return(
    tibble::tibble(
      system = NA_character_,
      system_score = NA_real,
      activity = NA_character_,
      activity_score = NA_real_
      )
    )
  
  term_lower <- str_to_lower(term)
  
  # --- SYSTEM MATCHES ---
  system_matches <- thinking_df |>
    mutate(match = str_detect(term_lower, stub)) |>
    filter(match) |>
    group_by(thinking) |>
    summarise(system_score = sum(weight), .groups = "drop")
  
  if (nrow(system_matches) == 0) {
    system_matches <- tibble(thinking = "Vague", system_score = 0)
  }
  
  # --- ACTIVITY MATCHES ---
  activity_matches <- activity_df |>
    mutate(match = str_detect(term_lower, stub)) |>
    filter(match) |>
    group_by(activity) |>
    summarise(activity_score = sum(weight), .groups = "drop")
  
  if (nrow(activity_matches) == 0) {
    activity_matches <- tibble(activity = "Unknown", activity_score = 0)
  }
  
  # --- COMBINE SYSTEM & ACTIVITY ---
  result <- tidyr::expand_grid(
    system_matches,
    activity_matches
  )
  
  # --- NORMALIZE SCORES ---
  if (normalize) {
    if (!all(is.na(result$system_score))) {
      result <- result |> mutate(system_score = system_score / max(system_score))
    }
    if (!all(is.na(result$activity_score))) {
      result <- result |> mutate(activity_score = activity_score / max(activity_score))
    }
  }
  
  return(result)
}
