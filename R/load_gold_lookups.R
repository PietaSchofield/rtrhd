#' load cprd files 
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
#'
#' @export
load_gold_lookups <- function(pddir,dbf,ow=T,db=F,inctxt="TXTFILES",dropcodes=T,prefix="gold"){
  if(F){
    pddir <- cpath
    dbf <- sgdb
    ow=F
    db=F
    inctxt="TXTFILES"
  }
  tabs <- list_tables(dbf=dbf)
  cprdfiles <- list.files(pddir,pattern=".*txt",full=T)
  if(dropcodes){
    cprdfiles <- cprdfiles[!grepl(".*(prod|med)codes[.]txt$",cprdfiles)]
  }
  names(cprdfiles) <- paste0(prefix,"_",tolower(gsub("(^[0-9]*_|[.]txt)","",basename(cprdfiles))))
  lapply(names(cprdfiles),function(fn){
    if(!fn%in%tabs || ow){
      dat <- readr::read_tsv(cprdfiles[[fn]],
                             col_types=readr::cols(.default=readr::col_character()),
                             locale=locale(encoding="ISO-8859-1")) 
      problems(dat)
      names(dat) <- tolower(names(dat))
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      duckdb::dbDisconnect(dbi,shutdown=T)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
    }else{
      nr <- get_table(dbf=dbf,sqlstr=paste0("SELECT COUNT(*) AS occur FROM ",fn)) %>% pull(occur)
      ext <- "records exist"
    }
    cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
  })
  if(!is.null(inctxt)){
    txttab_name <- paste0(prefix,"_txtlookups")
    cprdfiles <- list.files(file.path(pddir,inctxt),pattern=".*txt",full=T)
    names(cprdfiles) <- tolower(gsub("(^[0-9]*_|[.]txt)","",basename(cprdfiles)))
    if(!txttab_name%in%tabs || ow){
      dat <- lapply(cprdfiles,function(fn){
        dat <- readr::read_tsv(fn,
                             col_types=readr::cols(.default=readr::col_character()),
                             locale=locale(encoding="ISO-8859-1")) %>%
          as_tibble()  
        names(dat) <- c("key","value")
        dat
        })  %>% plyr::ldply() %>% dplyr::rename(tab=.id)
        load_table(dbf=dbf,dataset=dat,tab_name=txttab_name)
        nr <- dat %>% nrow()
        ext <- "new records"
        rm(dat)
        gc()
      }else{
        nr <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",txttab_name))
        duckdb::dbDisconnect(dbi,shutdown=T)
        ext <- "records exist"
      }
      cat(paste0(txttab_name,": ",nr," ",ext,"\n"))
  }
  return()
}
