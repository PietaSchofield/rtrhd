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
load_table <- function(filename=NULL,dataset=NULL,dbf,ow=F,db=F,
                       tab_name=gsub("(^[0-9]*_|[.].*)","",basename(filename)), 
                       selvars=NULL,delim="\t"){
  nrec <- 0
  if(!is.null(filename)){
    if(file.exists(filename)){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      if(!tab_name%in%duckdb::dbListTables(dbi) || ow){
        if(tab_name%in%duckdb::dbListTables(dbi)) DBI::dbExecute(dbi,paste0("DROP TABLE ",tab_name))
        dat <- readr::read_delim(filename,col_types=readr::cols(.default=readr::col_character()),
                                 delim=delim) 
        if(!is.null(selvars)) dat <- dat %>% dplyr::select(dplyr::all_of(selvars)) 
        if(!db) duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=T,append=F)
        nrec <- dat %>% nrow()
        cat(paste0(basename(filename),": ",nrec," records loaded\n"))
        rm(dat)
        duckdb::dbDisconnect(dbi,shutdown=T)
        gc()
      }else{
        cat(paste0(tab_name," exists\n"))
      }
    }else{
      cat(paste0(filename," not found\n"))
    }
  }else{
    if(!is.null(dataset)){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      if(!tab_name%in%duckdb::dbListTables(dbi) || ow){
        if(tab_name%in%duckdb::dbListTables(dbi)) DBI::dbExecute(dbi,paste0("DROP TABLE ",tab_name))
        dat <- dataset
        names(dataset) <- tolower(names(dataset))
        if(!is.null(selvars)) dat <- dat %>% dplyr::select(dplyr::all_of(selvars)) 
        if(!db) duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=T,append=F)
        nrec <- dat %>% nrow()
        cat(paste0(tab_name,": ",nrec," records loaded\n"))
        rm(dat)
        duckdb::dbDisconnect(dbi,shutdown=T)
      }
    }else{
      cat(paste("Nothing to load\n"))
    }
  }
  return(cat(paste0(nrec," records processed\n")))
}
