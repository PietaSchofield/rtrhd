#' Assign one or more body‑system labels to a term
#'
#' @param term A character vector (one element per concept label/synonym).
#' @param patterns Optional pre‑compiled pattern list (see
#'        [load_system_patterns()]).  If omitted the function will load the
#'        default bundled CSV on first call and cache the result.
#' @return A character vector of the same length as `term`.  Each element is
#'         either a pipe‑separated list of matching system classes
#'         (e.g. `"Neurological|Vascular"`) or `"Vague"` when nothing matches.
#' @export
#' @examples
#' assign_system(c("myocardial infarction", "headache", "apple"))
#' #> [1] "Cardio"               "Neurological|Vascular" "Vague"
assign_class <- function(term, patterns = NULL) {
  if (!is.character(term)) {
    stop("`term` must be a character vector", call. = FALSE)
  }

  # Lazily load & cache the pattern list
  if (is.null(patterns)) {
    if (!exists(".system_patterns_cache", envir = .GlobalEnv, inherits = FALSE)) {
      assign(".system_patterns_cache", load_system_patterns(),
             envir = .GlobalEnv)
    }
    patterns <- get(".system_patterns_cache", envir = .GlobalEnv)
  }

  # `str_detect()` is already vectorised over both the string and the pattern.
  # We loop over the *classes* (usually < 30) and collect the matches.
  matches <- lapply(patterns, function(pat) stringr::str_detect(term, pat))

  # Turn the list of logical vectors into a matrix (rows = terms, cols = classes)
  matches_mat <- do.call(cbind, matches)
  colnames(matches_mat) <- names(patterns)

  # For each row, paste together the column names where the entry is TRUE
  res <- apply(matches_mat, 1, function(row) {
    sys <- colnames(matches_mat)[row]
    if (length(sys) == 0) {
      "Vague"
    } else {
      paste(sys, collapse = "|")
    }
  })

  res
}
