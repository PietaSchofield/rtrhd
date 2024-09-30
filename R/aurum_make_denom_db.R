#' make a demographic linkage database for control construction
#'
#' @export
aurum_make_denom_db <- function(dbn,denomdir,linkdir,db=F){
  if(db){
    aurumdir <- file.path(Sys.getenv("HOME"),"Projects","refdata","aurum",".data")
    linkdir <- file.path(aurumdir,"January_2022_Source_Aurum")
    denomdir <- file.path(aurumdir,"202406_CPRDAurum")
    dbn <- file.path(aurumdir,"denom_202406.duckdb")
  }

  tabs <- cprdaurumtools::list_tables(dbf=dbn)
  if(!"acceptable_patients"%in%tabs){
    acceptable_file <- list.files(denomdir,pattern="Accept",full=T,recur=T)
    acceptable <- acceptable_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      dplyr::mutate(yob=as.numeric(yob),
             emis_ddate=lubridate::dmy(emis_ddate),
             cprd_ddate=lubridate::dmy(cprd_ddate),
             regstartdate=lubridate::dmy(regstartdate),
             regenddate=lubridate::dmy(regenddate),
             lcd=lubridate::dmy(lcd))
    cprdaurumtools::load_table(dbf=dbn,dataset=acceptable,tab_name="acceptable_patients",ow=T)
    rm(acceptable)
    gc()
  }
 
  if(!"practices"%in%tabs){
    practice_file <- list.files(denomdir,pattern="Practice",full=T,recur=T)
    practices <- practice_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      dplyr::mutate(lcd=lubridate::dmy(lcd))
    cprdaurumtools::load_table(dbf=dbn,dataset=practices,tab_name="practices")
    rm(practices)
    gc()
  }

  if(!"linkages"%in%tabs){
    linkage_file <- list.files(linkdir,pattern="eligibil",full=T,recur=T)
    linkages <- linkage_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      dplyr::mutate(linkdate=lubridate::dmy(linkdate))
    cprdaurumtools::load_table(dbf=dbn,dataset=linkages,tab_name="linkages")
    rm(linkages)
    gc()
  }
  return(dbn)
}
