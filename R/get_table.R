#' get table
#'
#' get the patient data
#'
#' @export
get_table <- function(dbf,sqlstr=NULL,tabname=NULL,fields='*',whereclause=NULL){
  if(F){
    dbf <- dmddb
    tabname <- "drug_exposures"
    fields <- '*'
    whereclause <- NULL
    strsql <- vtm_sql
  }
  if(is.null(sqlstr)){
    strsql <- str_c("
      SELECT DISTINCT
      ",paste0(fields,collapse=", "),"
      FROM 
      ",tabname,";")

    if(!is.null(whereclause)){
      strsql <- cat(strsql,"\n",whereclause)
    }
  }else{
    strsql <- sqlstr
  }
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  pats <- DBI::dbGetQuery(dbi,strsql) %>% tibble()
  duckdb::dbDisconnect(dbi, shutdown=T)
  return(pats)
}
      
