#' drop table
#'
#' @export
drop_table <- function(dbf,tab_name){
  lapply(tab_name, function(tbn){
    dbc <- duckdb::dbConnect(duckdb::duckdb(),dbf,write=T)
    if(tbn %in% dbListTables(dbc)){
      sqlstr <- paste("DROP TABLE ",tbn)
      dbExecute(dbc,sqlstr)
    }
    dbDisconnect(dbc,shutdown=T)
  })
}
