#' make a full database
#'
#' This is a wrapper for the individual table load functions that loop through each extract table type
#' and imports the records into one table for each type. It is not meant for clever appending or updating
#' individual tables it is a either create or overwrite it. If called with overwrite (ow) it will delete
#' whole database and regenerate.
#'
#' @export
make_aurum_database <- function(dbName,dbPath,patient_path,lookup_path,db=F,ow=F){
  if(db){
    ow <- F
    dbName <- "synthetic.duckdb"
    dbPath <- file.path(Sys.getenv("HOME"),"Projects","rtrhd")
    patient_data <- file.path(dbPath,"Aurum_Synthetic_Data","patient_data")
    lookup_data <- file.path(dbPath,"Aurum_Synthetic_Data","lookups")
  }else{
   if(ow && file.exists(dbFile))
     unlink(dbFile)
  }
  aPath <- patient_data
  cPath <- lookup_data
  dbFile <- file.path(dbPath,dbName)
  if(file.exists(aPath)){
    rtrhd::load_aurum_patients(pdir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_observations(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_consultations(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_referrals(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_practices(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_drugissues(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_problems(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_staff(pddir=aPath,dbf=dbFile,ow=ow,add=F)
    rtrhd::load_aurum_lookups(pddir=cPath,dbf=dbFile,ow=ow)
  }
  return(dbFile)
}
