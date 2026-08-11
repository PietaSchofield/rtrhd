#' List tables in the database
#'
#' @export
list_tables <- function(dbf){
  dbc <- duckdb::dbConnect(duckdb::duckdb(shared_home = FALSE),dbf)
  ret <- duckdb::dbListTables(dbc)
  duckdb::dbDisconnect(dbc,shutdown=T)
  ret
}
