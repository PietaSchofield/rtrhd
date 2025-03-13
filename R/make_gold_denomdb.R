#' make a demographic linkage database for control construction
#'
#' @export
make_gold_denomdb <- function(dbn,denomdir,linkdir,db=F){
  if(db){
    golddir <- file.path(Sys.getenv("HOME"),"Projects","refdata","gold",".data")
    linkdir <- file.path(golddir,"January_2022_Source_GOLD")
    denomdir <- file.path(golddir,"202409_CPRDGold")
    dbn <- file.path(golddir,"202409_gold.duckdb")
  }

  tabs <- cprdgoldtools::list_tables(dbf=dbn)
  if(!"acceptable_patients"%in%tabs){
    acceptable_file <- list.files(denomdir,pattern="accept",full=T,recur=T)
    acceptable <- acceptable_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      mutate(yob=as.numeric(yob),
             frd=lubridate::dmy(frd),
             crd=lubridate::dmy(crd),
             tod=lubridate::dmy(tod),
             deathdate=lubridate::dmy(deathdate),
             pracid=stringr::str_sub(patid,5))

    cprdgoldtools::load_table(dbf=dbn,dataset=acceptable,tab_name="acceptable_patients",ow=T)
    rm(acceptable)
    gc()
  }
 
  if(!"practices"%in%tabs){
    practice_file <- list.files(denomdir,pattern="practice",full=T,recur=T)
    practices <- practice_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      mutate(lcd=lubridate::dmy(lcd),
             utc=lubridate::dmy(uts))
    cprdaurumtools::load_table(dbf=dbn,dataset=practices,tab_name="practices")
    rm(practices)
    gc()
  }

  if(!"linkages"%in%tabs){
    linkage_file <- list.files(linkdir,pattern="eligibil",full=T,recur=T)
    linkages <- linkage_file %>% readr::read_tsv(col_type=cols(.default=col_character())) %>%
      mutate(linkyear=as.integer(linkyear))
     # mutate(linkdate=lubridate::dmy(linkdate))
    cprdaurumtools::load_table(dbf=dbn,dataset=linkages,tab_name="linkages")
    rm(linkages)
    gc()
  }
  return(dbn)
}
