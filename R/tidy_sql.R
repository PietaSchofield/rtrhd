#' tidy up the sql
#'
#' @export
tidy_sql <- function(sql, prefix = "tmp",
  # Define common SQL keywords (add more as needed)
  sql_keywords=c(
    "SELECT", "FROM", "WHERE", "AND", "OR", "GROUP", "ORDER", "HAVING",
    "INSERT", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "DROP",
    "WITH","INNER","LEFT","RIGHT","OUTER","FULL","CROSS","/\\*","\\*/"
  )){
  # Create a regex pattern to detect lines starting with SQL keywords
  keyword_pattern <- paste0("^\\s*(", paste(sql_keywords, collapse = "|"), ")\\b")
  # Pattern to detect table names with the "tmp_" prefix
  tmptab_pattern <- paste0("^\\s*",prefix,"_")
  
  # Split the SQL into lines while preserving double line breaks
  lines <- strsplit(sql, "(?<=\n)", perl = TRUE)[[1]]
  
  # Tidy and reformat each line
  tidied_lines <- list()
  current_indent <- 0 # Track the current level of indentation
  in_with_block <- FALSE
  
  for (line in lines) {
    original_line <- line # Keep the original line to preserve line breaks
    line <- trimws(line)
 
    if (grepl("^WITH\\b", line, ignore.case = TRUE)) {
      # Start of a WITH block
      in_with_block <- TRUE
      tidied_lines <- c(tidied_lines, line)
      current_indent <- 2
      next
    }
    
    if (in_with_block) {
      if (grepl(tmptab_pattern, line, ignore.case = TRUE)) {
        # tmp_ tables in WITH block: align at current indentation
        tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent), line))
      } else if (nchar(line) > 0) {
        # Other lines in WITH block: increment indentation by 2 spaces
        if (grepl(keyword_pattern, line, ignore.case = TRUE)) {
          tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent + 2), line))
          current_indent <- 2 # Reset indentation for the new keyword block
        } else if (nchar(line) == 0) {
          # Preserve double line breaks
          tidied_lines <- c(tidied_lines, "")
        } else {
          # Non-keyword lines: indent 2 spaces more than the previous keyword/table line
          tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent + 4), line))
        }
      }
      
      # End of WITH block
      if (grepl("\\)\\s*$", line)) {
        in_with_block <- FALSE
        current_indent <- 0
      }
      next
    }
    
    # Check if the line starts with an SQL keyword
    if (grepl(keyword_pattern, line, ignore.case = TRUE)) {
      tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent), line))
      current_indent <- 2 # Reset indentation for the new keyword block
    } else if (grepl(tmptab_pattern, line, ignore.case = TRUE)) {
      # Align `tmp_` table names
      tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent + 2), line))
    } else if (nchar(line) == 0) {
      # Preserve double line breaks
      tidied_lines <- c(tidied_lines, "")
    } else {
      # Non-keyword lines: indent 2 spaces more than the previous keyword/table line
      tidied_lines <- c(tidied_lines, paste0(strrep(" ", current_indent + 2), line))
    }
  }
  
  # Combine the tidied lines into a single string
  tidied_sql <- paste(tidied_lines, collapse = "\n")
  return(tidied_sql)
}

