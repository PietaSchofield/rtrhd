#' Load the aurum lookups files into a database
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
aurum_load_lookups <- function(txtdir,dbf,ow=F,db=F,silent=T){
  if(F){
    txtdir <- cpath
    dbf <- sadb
    ow <- F
    silent <- T
    db <- F
  }
  tabs <- list_tables(dbf=dbf)
  cprdfiles <- list.files(txtdir,pattern=".*txt",full=T,recur=T)
  names(cprdfiles) <- paste0("aurum_",tolower(gsub("(^[0-9]*_|[.]txt)","",basename(cprdfiles))))
  lapply(names(cprdfiles),function(fn){
    if(!fn%in%tabs || ow){
      dat <- readr::read_tsv(cprdfiles[[fn]],
                             col_types=readr::cols(.default=readr::col_character()),
                             locale=locale(encoding="ISO-8859-1")) %>%
        as_tibble()
      names(dat) <- tolower(names(dat))
      load_table(dbf=dbf,dataset=dat,tab_name=fn,ow=ow)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
      cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
    }else if(!silent){
      nr <- get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) AS occur FROM ",fn)) %>% pull(occur)
      ext <- "records exist"
      cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
    }
  })
  return()
}
