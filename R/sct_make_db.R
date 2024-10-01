#' make a snomed database
#'
#' Trying to use some of my existing convenience functions
#'
#' @importFrom magrittr %>%
#'
#' @export
sct_make_db <- function(dbPath,txtPath,db=F,ow=T,preterm=NULL,posterm=NULL,prerefs=NULL,posrefs=NULL,
  incterm=c("concept","description","relationship","textdefinition"),
  increfs=c("language","association","extendedmap","simplemap","extendedmap_1","simple")){
  if(db){
     require(magrittr)
     dbPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","snomed")
     txtPath <- file.path(Sys.getenv("HOME"),"Projects","refdata","snomed","20240828_mono")
     db <- F
     ow <- F
     posterm <- "_MONOSnap.*"
     preterm <- "sct2_"
     posrefs <- "MONOSnap.*"
     prerefs <- ".*efset_"
     incterm <- NULL
     increfs <- NULL
     if(T){
       incterm <- c("concept","description","relationship","textdefinition")
       increfs <- c("language","association","extendedmap","simplemap","extendedmap_1","simple")
     }
  }
  dbFile <- file.path(dbPath,paste0("sct_",basename(txtPath),".duckdb"))
  # terminology files
  files <- list.files(txtPath,pattern="sct.*txt",recur=T,full=T)
  #
  # the names are horrible and do not follow a consistent convention
  #
  names(files) <- gsub(paste0("(",preterm,"|",posterm,")"),"",basename(files))
  names(files) <- gsub(paste0("(",prerefs,"|",posrefs,")"),"",names(files)) %>% tolower() %>% 
    make.names(unique=T)
  names(files) <- paste0("terminology_",gsub("\\.","_",names(files))) 
  if(is.null(incterm)) incterm <- names(files)
  res <- lapply(incterm,rtrhd::load_sct_file,dbf=dbFile,filelist=files)
  # refset files
  files <- list.files(txtPath,pattern="der.*txt",recur=T,full=T)
  names(files) <- gsub(paste0("(",prerefs,"|",posrefs,")"),"",basename(files)) %>% tolower() %>%
    make.names(unique=T)
  names(files) <- paste0("refset_",gsub("\\.","_",names(files)))
  if(is.null(increfs)) increfs <- names(files)
  res <- lapply(increfs,rtrhd::load_sct_file,dbf=dbFile,filelist=files)

  return(dbFile) 
}

#' @export
load_sct_file <- function(tab,dbf,filelist){
  rtrhd::load_table(filename=filelist[[tab]],dbf=dbf,tab_name=tab) 
}

