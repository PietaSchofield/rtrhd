#' make a snomed database
#'
#' Trying to use some of my existing convenience functions
#'
#' @importFrom magrittr %>%
#'
#' @export
icd_make_db <- function(dbPath,txtPath,dbName=NULL,db=F,ow=T,
                        preterm=NULL,posterm=NULL,
                        prerefs=NULL,posrefs=NULL){
  if(db){
     dbPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","icd10")
     txtPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","icd10")
     db <- F
     ow <- F
     posrefs <- "_GB_2016.*$"
     prerefs <- ".*Edition5_"
     increfs <- NULL
  }
  if(is.null(dbName)){
    dbFile <- file.path(dbPath,paste0("sct_",basename(txtPath),".duckdb"))
  }else{
    dbFile <- file.path(dbPath,dbName)
  }
  # terminology files
  files <- list.files(txtPath,pattern="ICD10.*txt",recur=T,full=T)
  #
  # the names are horrible and do not follow a consistent convention
  #
  names(files) <- gsub(paste0("(",prerefs,"|",posrefs,")"),"",files) %>% tolower() %>% 
    make.names(unique=T)
  names(files) <- paste0("icd_",gsub("\\.","_",names(files))) 
  increfs <- names(files)
  res <- lapply(increfs,rtrhd::load_icd_file,dbf=dbFile,filelist=files)

  return(dbFile) 
}

#' Load icd10 file
#'
#' @export
load_icd_file <- function(tab,dbf,filelist){
  rtrhd::load_table(filename=filelist[[tab]],dbf=dbf,tab_name=tab) 
}

