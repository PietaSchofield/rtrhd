#' make a full database
#'
#' @export
make_aurum_database <- function(dbName,dbPath,patient_path,lookup_path,db=F,ow=F){
  if(db){
    dbName <- "synthetic.duckdb"
    dbPath <- file.path(Sys.getenv("HOME"),"Projects","rtrhd")
    patient_data <- file.path(dbPath,"Aurum_Synthetic_Data","patient_data")
    lookup_data <- file.path(dbPath,"Aurum_Synthetic_Data","lookups")
  }else{
   if(ow && file.exists(dbFile))
     unlink(dbFile)
  }
  aPath <- patient_path
  cPath <- lookup_path
  dbFile <- file.path(dbPath,dbName)
  if(file.exists(aPath)){
    rtrhd::load_aurum_patients(pdir=aPath,dbf=dbFile)
    rtrhd::load_aurum_observations(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_consultations(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_referrals(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_practices(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_drugissues(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_problems(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_staff(pddir=aPath,dbf=dbFile)
    rtrhd::load_aurum_lookups(pddir=cPath,dbf=dbFile)
  }
  return(dbFile)
}
