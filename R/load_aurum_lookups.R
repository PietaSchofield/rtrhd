#' load the aurum lookup tables
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param bdir the name of the batch subdirectory
#' @param odir the name of the output directory
#' @param olist the list of observation codes
#' @param bpp BiocParallal Multicore Parameters
#'
#'
#' Pass a table of covariate codes and generate covariates table
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
