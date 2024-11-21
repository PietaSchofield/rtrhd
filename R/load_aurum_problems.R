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
load_aurum_problems <- function( pddir,dbf,ow=F,db=F,tab_name="problems",add=F,
    selvars=c("patid","obsid","parentprobobsid","probenddate","expduration","lastrevdate",
              "parentprobrelid","probstatusid")){
  if(F){
    pddir <- aPath
    dbf <- dbFile
    ow <- F
    db <- F
    add <- T
    tab_name <- "problems"
    selvars <- c("patid","obsid","parentprobobsid","probenddate","expduration","lastrevdate",
              "parentprobrelid","probstatusid")
  }
  if(ow && add){
    stop("Error overwrite and append both true\n")
    return()
  }
  tabs <- rtrhd::list_tables(dbf=dbf)
  nrec <- 0
  if(!tab_name%in%tabs || ow || add){
    profiles <- list.files(pddir,pattern="Prob.*txt$",full=T,recur=T)
    if(tab_name%in%tabs && ow){
      rtrhd::sql_execute(dbf=dbf,sql_make=paste0("DROP TABLE IF EXISTS ",tab_name,";"))
    }
    nrec <- lapply(profiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars)) %>%
        dplyr::mutate(probeddate = format(lubridate::dmy(probenddate)),
                      lastrevdate = format(lubridate::dmy(lastrevdate)))
      rtrhd::load_table(dbf=dbf,tab_name=tab_name,dataset=dat,ow=F,append=T)
      nr <- dat %>% nrow()
      rm(dat)
      gc()
      return(nr)
    })
    ext <- "new records"
  }else{
    nrec <- rtrhd::get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
