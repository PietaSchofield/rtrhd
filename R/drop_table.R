#' drop table
#'
#' @export
drop_table <- function(dbif,tabnames){
  lapply(tabnames, function(tbn){
    dbc <- duckdb::dbConnect(duckdb::duckdb(),dbif,write=T)
    if(tbn %in% dbListTables(dbc)){
      sqlstr <- paste("DROP TABLE ",tbn)
      dbExecute(dbc,sqlstr)
    }
    dbDisconnect(dbc,shutdown=T)
  })
}
