##' Classify a SNOMED term into system (clinical thinking) and activity
#'
#' Uses regex stub dictionaries to classify a SNOMED CT term into:
#' \itemize{
#'   \item Clinical system / "what the clinician was thinking" (e.g. MSK, Respiratory)
#'   \item Type of activity (e.g. Imaging, Referral, Procedure)
#' }
#'
#' @param term A character string representing a SNOMED term.
#' @param thinking_df A data frame with columns:
#'   \describe{
#'     \item{stub}{Regex pattern}
#'     \item{thought}{Clinical system label}
#'   }
#' @param activity_df A data frame with columns:
#'   \describe{
#'     \item{stub}{Regex pattern}
#'     \item{activity}{Activity label}
#'   }
#'
#' @return A tibble with columns:
#'   \describe{
#'     \item{term}{Input SNOMED term}
#'     \item{system}{Matched clinical system(s)}
#'     \item{activity}{Matched activity type(s)}
#'   }
#'   Returns all combinations if multiple matches occur.
#'
#' @importFrom stringr str_detect str_to_lower
#' @importFrom dplyr filter mutate pull
#' @importFrom tidyr expand_grid
#' @importFrom tibble tibble
#'
#' @export
classify_snomed_term <- function(term, thinking_df, activity_df) {

  # Normalize input
  term_clean <- stringr::str_to_lower(term)

  # Match clinical thinking (system)
  systems <- thinking_df %>%
    dplyr::mutate(match = stringr::str_detect(term_clean, stub)) %>%
    dplyr::filter(match) %>%
    dplyr::pull(thought) %>%
    unique()

  # Match activity
  activities <- activity_df %>%
    dplyr::mutate(match = stringr::str_detect(term_clean, stub)) %>%
    dplyr::filter(match) %>%
    dplyr::pull(activity) %>%
    unique()

  # Handle no matches
  if (length(systems) == 0) systems <- "Vague"
  if (length(activities) == 0) activities <- "Vague"

  # Return all combinations
  tidyr::expand_grid(
    system = systems,
    activity = activities
  )
}
