#' tidy up the sql
#'
#' @export
tidy_sql <- function(sql,
  sql_keywords=c(
    "SELECT", "FROM", "WHERE", "AND", "OR", "GROUP", "ORDER", "HAVING","INNER","LEFT","RIGHT", 
    "INSERT", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "DROP", "OUTER", "FULL")){
  # Create a regex pattern to detect lines starting with SQL keywords
  keyword_pattern <- paste0("^\\s*(", paste(sql_keywords, collapse = "|"), ")\\b")
  
  # Split the SQL into lines
  lines <- strsplit(sql, "\n")[[1]]
  
  # Tidy and reformat each line
  tidied_lines <- sapply(lines, function(line) {
    # Trim whitespace from the line
    line <- trimws(line)
    # Check if the line starts with an SQL keyword
    if (grepl(keyword_pattern, line, ignore.case = TRUE)) {
      # If it starts with a keyword, leave it aligned
      return(toupper(line))
    } else {
      # If it doesn't, indent it by 2 spaces
      return(paste0("  ", line))
    }
  })
  
  # Combine the tidied lines into a single string
  tidied_sql <- paste(tidied_lines, collapse = "\n")
  return(tidied_sql)
}
