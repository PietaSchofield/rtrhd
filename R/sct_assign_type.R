#' SNOMED CT Type Classifierhttps://teams.microsoft.com/v2/
#'
#' Assign a SNOMED‑CT type (category) to a term
#'
#' @param term A character string (the concept label or synonym)
#' @return A character string with **one** of the following values:
#'        \code{Imaging}, \code{Investigation}, \code{Referral},
#'        \code{Advice}, \code{Follow-up/Discharge}, \code{Procedure},
#'        \code{Admin}, \code{Presentation} (catch‑all), or \code{Vague}
#'        if no pattern matches.
#' @export
assign_type <- function(term) {
  # Normalise for case‑insensitive matching
  term <- str_to_lower(term)

  # Ordered list of regex patterns – the first match wins
  type_patterns <- list(
    Imaging = paste0("x-ray|ultrasound|scan|\\becg\\b|radiograph|ogram|echo"),
    Investigation = paste0("test|level|measurement|result|sample|swab|lab|analysis|count|",
                                 "serum|plasma|urine|blood|biopsy|exam"),
    Referral = paste0("refer|admit|admiss|follow-up|discharge"),
    Advice = paste0("advice|reassurance|warning|chat"),
    Procedure = paste0("procedure|removal|repair|dressing|insertion|surgery|manipulation|",
                             "irrigation"),
    Admin = paste0("letter|telephone|message")
  )

  split_patterns <- lapply(type_patterns,function(x) strsplit(x,"[|]")[[1]]) 

  pattern_df <- dplyr::bind_rows(
    lapply(names(split_patterns), function(x){
       data.frame(
          stub = split_patterns[[x]],
          thought = x,
          stringsAsFactors = FALSE
        )
      })
    )

  if(F){
    pattern_df
    pattern_df |> dplyr::arrange(stub) |> 
      readr::write_csv(file.path(Sys.getenv("HOME"),"Projects","sprint","refs",
                                 "type_pattern_classes.csv"))
  }
  
  # Find the first pattern that matches
  match_idx <- sapply(type_patterns, function(pat) str_detect(term, pat))
  if(length(names(which(match_idx))) >= 1){
    type_map <- paste0(names(which(match_idx[1:length(match_idx)])),collapse="|")
  }else{
    type_map <- "Presentation"
  }

  # Otherwise return the name of the matching pattern
  return(type_map)
}

