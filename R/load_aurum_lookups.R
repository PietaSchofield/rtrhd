#' load the aurum lookup tables
#'
#'
#' Pass a table of covariate codes and generate covariates table
#'
#' @import dplyr
#' @import readr
#'
#' @export
load_aurum_lookups <- function(pddir,dbf,ow=F,db=F,silent=T,prefix="ref"){
  if(F){
    pddir <- cPath
    dbf <- dbFile
    ow <- F
    silent <- T
    db <- F
  }
  tabs <- rtrhd::list_tables(dbf=dbf)
  cprdfiles <- list.files(pddir,pattern=".*txt",full=T)
  names(cprdfiles) <- paste0(prefix,"_",tolower(gsub("(^[0-9]*_EMIS|[.]txt)","",basename(cprdfiles))))
  lapply(names(cprdfiles),function(fn){
    if(!fn%in%tabs || ow){
      dat <- readr::read_tsv(cprdfiles[[fn]],
                             col_types=readr::cols(.default=readr::col_character()),
                             locale=locale(encoding="ISO-8859-1")) %>%
        as_tibble()
      dat <- dat %>% 
        mutate(across(where(is.character), ~ iconv(iconv(.,to="latin1", from= "UTF-8"),
                                                   to="latin1",from="UTF-8")))
      names(dat) <- tolower(names(dat))
      rtrhd::load_table(dbf=dbf,dataset=dat,tab_name=fn,ow=ow)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
    }else if(!silent){
      nr <- rtrhd::get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) AS occur FROM ",fn)) %>% pull(occur)
      ext <- "records exist"
    }
  })
  return()
}
