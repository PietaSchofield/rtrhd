# get observations batch
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
#' @import magrittr
#' @export
load_aurum_staff <- function(pddir,dbf,ow=F,db=F,tab_name="staff",add=F){
  if(F){
    pddir <- aPath
    dbf <- dbFile
    ow <- F
    db <- F
    add <- T
    tab_name <- "staff"
  }
  if(ow && add){
    stop("Error overwrite and append both true\n")
    return()
  }
  tabs <- rtrhd::list_tables(dbf=dbf)
  nrec <- 0
  if(!tab_name%in% tabs || ow || add){
    stafiles <- list.files(pddir,pattern="Staff.*txt$",full=T,recur=T)
    extent <- NULL
    if(tab_name%in% tabs && ow){
      rtrhd::sql_execute(dbf=dbf,sql_make=paste0("DROP TABLE IF EXISTS ",tab_name,";"))
    }
    dat <- lapply(stafiles,function(fn){
             dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) 
           }) %>% dplyr::bind_rows() %>% unique() 
    rtrhd::load_table(dbf=dbf,tab_name=tab_name,dataset=dat,ow=F)
    nred <- dat %>% nrow()
    rm(dat)
    gc()
    ext <- "new records"
  }else{
    nrec <- rtrhd::get_table(dbf=dbf,paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
