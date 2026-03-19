#' Load system‑stub CSV and compile regexes
#'
#' @param path Path to the CSV file.  Defaults to the bundled example
#'        `inst/extdata/snomed_system_stubs.csv` when the function is
#'        called from inside a package.
#' @return A named list where each element is a **pre‑compiled** regex
#'         object (as returned by [stringr::regex()]) for the corresponding
#'         class.  The list also contains an element `"Vague"` whose value
#'         is `NULL` (used only as a sentinel).
#'
#' @export
load_system_patterns <- function(path = NULL) {
  if (is.null(path)) {
    # When inside a package we can use system.file()
    pkg <- "rtrhd"                     # <-- change to your package name
    path <- system.file("extdata", "snomed_system_stubs.csv",
                        package = pkg, mustWork = TRUE)
  }

  # Read CSV – we expect exactly two columns: class, stub
  stubs <- readr::read_csv(
    path,
    col_types = readr::cols(
      class = readr::col_character(),
      stub  = readr::col_character()
    ),
    show_col_types = FALSE
  )

  # Remove completely empty stubs (they will be treated as the Vague fallback)
  stubs <- dplyr::filter(stubs, !is.na(stub) & stub != "")

  # Group by class and collapse stubs with "|"
  pats <- stubs %>%
    dplyr::group_by(class) %>%
    dplyr::summarise(
      pattern = paste0(stub, collapse = "|"),
      .groups = "drop"
    ) %>%
    # Build a regex object – ignore_case = TRUE makes the matching
    # case‑insensitive without having to call str_to_lower() inside the
    # hot loop.
    dplyr::mutate(regex = purrr::map(pattern,
                                    ~ stringr::regex(.x, ignore_case = TRUE))) %>%
    deframe()   # named list: class -> compiled regex

  # Add the Vague sentinel (value NULL – we will treat it specially)
  pats[["Vague"]] <- NULL

  pats
}
