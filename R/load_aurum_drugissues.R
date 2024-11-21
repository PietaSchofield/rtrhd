# get_drugissues_batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#' @param bpp BiocParallal Multicore Parameters
#'
#' @export
load_aurum_drugissues <- function(pddir,dbf,ow=F,db=F,tab_name="drug_issues",add=F,
    selvars2=c("patid","prodcodeid","issuedate","dosageid","quantity","quantunitid","duration")){
  if(F){
    pddir <- aPath
    dbf <- dbFile
    ow <- F
    db <- F
    add <- T
    tab_name <- "drug_issues"
    selvars2 <- c("patid","prodcodeid","issuedate","dosageid","quantity","quantunitid","duration")
  }
  if(ow && add){
    stop("Error both overwrite and append true\n")
    return()
  }
  tabs <- rtrhd::list_tables(dbf=dbf)
  nrec <- 0
  if(!tab_name%in%tabs || ow || add){
    difiles <- list.files(pddir,pattern="Drug",full=T,recur=T)
    if(tab_name%in%tabs && ow){
      rtrhd::sql_execute(dbf=dbf,sql_make=paste0("DROP TABLE IF EXISTS ",tab_name,";"))
    }
    nrec <- lapply(difiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars2)) %>%
        dplyr::mutate(issuedate = format(lubridate::dmy(issuedate))) 
      rtrhd::load_table(dbf=dbf,tab_name=tab_name,dataset=dat,ow=F,append=T)
      nr <- dat %>% nrow()
      rm(dat)
      gc()
    })
    ext <- "new records"
  }else{
    nrec <- rtrhd::get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
