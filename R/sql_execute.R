#' execute and sql file in a duckdb
#'
#' @export
sql_execute <- function(dbf,sql_make,sql_query=NULL,sql_drop=NULL,db=F){
  if(db){
    dbf <- adb
    sql_make <- make_sql
    sql_make
    sql_query <- query_sql
    sql_query
    sql_drop <- drop_sql
    sql_drop
  }
  # Connect to your DuckDB database
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = dbf, write=T)

  # Register and execute the external SQL script
  nret <- DBI::dbExecute(con, sql_make)

# Fetch results if the query is a SELECT
  if(!is.null(sql_query)){
    res <- DBI::dbGetQuery(con, sql_query) %>% as_tibble()
  }else{
    res <- nret
  }
  if(!is.null(sql_drop)){
    nret <- DBI::dbExecute(con, sql_drop)
  }
  duckdb::dbDisconnect(con,shutdown=T)
  return(res)
}
