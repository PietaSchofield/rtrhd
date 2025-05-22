#' list database
#'
#' @export
list_db <- function(dbf, incv=F){
  if(F){
    dbf=adb
  }
  if(incv){
    tables <- rtrhd::list_tables(dbf=dbf) 
  }else{
    sql_query <- glue::glue("
      SELECT table_name
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
      AND table_schema = 'main'
    ",.trim=TRUE)

    tables <- rtrhd::get_table(dbf=dbf,sqlstr=sql_query) %>% pull(table_name)
  }
  tables  %>% lapply(function(tn){
    tibble(
      tablename=tn,
      fields=rtrhd::list_fields(dbf=dbf,tab=tn) %>% paste0(collapse=", "),
      records=rtrhd::get_table(dbf=dbf,sqlstr=paste("SELECT COUNT(*) AS r FROM",tn)) %>% pull(r)
    )
  }) %>% bind_rows()
}
