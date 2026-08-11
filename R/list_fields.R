#' List tables in the database
#'
#' @export
list_fields <- function(tab_name,dbf){
  dbc <- duckdb::dbConnect(duckdb::duckdb(shared_home = FALSE),dbf)
  ret <- duckdb::dbListFields(dbc,tab_name)
  duckdb::dbDisconnect(dbc,shutdown=T)
  ret
}
