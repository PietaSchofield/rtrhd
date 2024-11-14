#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
load_aurum_patients <- function(pdir,dbf,ow=F,db=F,tab_name="patients",add=F,
   selvars1=c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")){
  if(db){
    tab_name <- "patients"
    ow <- F
    dbf <- dbFile
    pdir <- aPath
    add <- T
   selvars1 <- c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")
  }
  if(add & ow){
    stop("Error both overwrite and append set. \n")
    return()
  }
  tabs <- rtrhd::list_tables(dbf)
  if(!tab_name%in%tabs || ow || add){
    patfiles <- list.files(pdir,pattern="Patient",full=T,recur=T)
    dat <- lapply(patfiles,function(fn){
       readr::read_tsv(fn,col_type=readr::cols(.default=readr::col_character())) %>%
                     dplyr::select(all_of(selvars1)) %>%
                     dplyr::mutate(yob = as.integer(yob),
                            emis_ddate = format(lubridate::dmy(emis_ddate)),
                            regstartdate = format(lubridate::dmy(regstartdate)),
                            regenddate = format(lubridate::dmy(regenddate)),
                            cprd_ddate = format(lubridate::dmy(cprd_ddate)))
                     }) %>% bind_rows()
    nrec <- dat %>% nrow()
    rtrhd::load_table(dbf=dbf,tab_name=tab_name,dataset=dat,ow=F)
    rm(dat)
    gc()
    ext <- "new records"
  }else{
    nrec <- rtrhd::get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(nrec)
}
