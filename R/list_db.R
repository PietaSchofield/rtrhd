#' list database
#'
#' @export
list_db <- function(dbf){
  rtrhd::list_tables(dbf) %>% lapply(function(tn){
    tibble(
      tablename=tn,
      fields=rtrhd::list_fields(dbf=dbf,tab=tn) %>% paste0(collapse=", "),
      records=rtrhd::get_table(dbf=dbf,sqlstr=paste("SELECT COUNT(*) AS r FROM",tn)) %>% pull(r)
    )
  }) %>% bind_rows()
}
