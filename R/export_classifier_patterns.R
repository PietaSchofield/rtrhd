#' Export a pattern list (regex objects or plain strings) to a key‑value CSV
#'
#' The resulting CSV has two columns:
#'   \describe{
#'     \item{class}{the name that should be returned for matches (e.g. "Urinary")}
#'     \item{stub}{the literal pattern string – pipe‑separated alternatives that were
#'                used to build the regex.  Empty stubs are interpreted by
#'                \code{\link{load_system_patterns}} as the \code{"Vague"} fallback.}
#'   }
#'
#' @param patterns A **named list**.  Each element may be
#'        * a pre‑compiled regex object (as returned by \code{\link[stringr]{regex}} or
#'          by \code{\link{load_system_patterns}}), **or**
#'        * a plain character string containing the pattern.
#'        The list names become the `class` column.
#' @param file Path to the CSV file to write (overwrites if it exists).
#' @examples
#' \dontrun{
#'   # Suppose you already have the list (maybe from load_system_patterns())
#'   sys_pats <- load_system_patterns()
#'
#'   # Export it – works whether sys_pats holds regex objects or plain strings
#'   export_system_patterns(sys_pats,
#'                         file = "inst/extdata/snomed_system_stubs.csv")
#' }
#' @export
export_system_patterns <- function(patterns, file) {
  # ---- Input validation ----------------------------------------------------
  if (!is.list(patterns)) {
    stop("`patterns` must be a list.", call. = FALSE)
  }

  # If the list has no names we fabricate a default class name ("Vague")
  # – this mirrors the fallback used by the loader.
  if (is.null(names(patterns))) {
    names(patterns) <- rep("Vague", length(patterns))
  }

  # ---- Extract the pattern string from each element ------------------------
  #   * If it is a regex object, attr(x, "pattern") holds the source pattern.
  #   * Otherwise we assume the element itself is the pattern string.
  stubs <- vapply(patterns,
                  function(x) {
                    pat <- attr(x, "pattern")      # works for regex objects
                    if (is.null(pat)) {            # not a regex object → plain string
                      pat <- as.character(x)
                    }
                    pat
                  },
                  FUN.VALUE = character(1),
                  USE.NAMES = FALSE)

  # ---- Build the two‑column data frame ------------------------------------
  out <- data.frame(
    class = names(patterns),
    stub  = stubs,
    stringsAsFactors = FALSE
  )

  # ---- Write CSV -----------------------------------------------------------
  readr::write_csv(out, file)

  # ---- Invisible return (handy for piping) ---------------------------------
  invisible(out)
}
