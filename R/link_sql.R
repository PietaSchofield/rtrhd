#' link sql
#'
#' create a temp sql file to include and display in a Rmarkdown document
#'
#' @export
link_sql <- function(sql_str){
  sql_file <- tempfile(fileext=".sql")
  writeLines(con=sql_file,sql_str)
  return(sql_file)
}

#' unlink sql
#'
#' delete the temporary sql file
#'
#' @export
unlink_sql <- function(sql_file){
  unlink(sql_file)
}
