#' make a snomed database
#'
#' Trying to use some of my existing convenience functions
#'
#' @export
sct_make_db <- function(dbPath,txtPath,db=F,ow=T,pref=NULL,postf=NULL,
                        include=c("concept","description","relationship","textdefinition")){
  if(db){
     dbPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","snomed")
     txtPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","snomed","20240828_mono")
     db <- F
     ow <- F
     postf <- "_MONOSnap.*"
     pref <- "sct2_"
     include <- c("concept","description","relationship","textdefinition")
  }
  dbFile <- file.path(dbPath,paste0(basename(dirname(txtPath)),".duckdb"))
  files <- list.files(txtPath,pattern="sct.*txt",recur=T,full=T)
  res <- lapply(files[include],load_sct_file,dbf=dbFile,postf=postf,pref=pref)
  return(dbFile) 
}

#' @export
load_sct_file <- function(fn,dbf,pref,postf){
  tab <- gsub(paste0("(",pref,"|",postf,")"),"",basename(fn)) %>% tolower()
  rtrhd::load_table(filename=fn,dbf=dbf,tab_name=tab) 
}

