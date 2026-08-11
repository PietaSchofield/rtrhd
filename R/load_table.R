#' Load a table
#'
#' @export
load_table <- function(filename=NULL,dataset=NULL,dbf=NULL,con=NULL,ow=F,db=F,append=F,
                       tab_name=gsub("(^[0-9]*_|[.].*)","",basename(filename)),
                       selvars=NULL,delim="\t",quote=""){
  nrec <- 0

  if(is.null(con) && is.null(dbf)){
    stop("load_table() needs either 'con' (an open connection) or 'dbf' (a path).")
  }

  ## Own the connection lifecycle only if we opened it ourselves.
  ## Was: every call opened + fully shut down its own connection —
  ## fine in isolation, but murder in a loop (see sct_make_db) where
  ## it meant hundreds of connect/shutdown cycles for one logical build.
  own_con <- is.null(con)
  dbi <- if (own_con) duckdb::dbConnect(duckdb::duckdb(shared_home = FALSE), dbf) else con

  if(!is.null(filename)){
    if(file.exists(filename)){
      if(!tab_name%in%duckdb::dbListTables(dbi) || ow || append){
        if(ow){
          DBI::dbExecute(dbi,paste0("DROP TABLE IF EXISTS ",tab_name))
        }
        dat <- readr::read_delim(filename,col_types=readr::cols(.default=readr::col_character()),
                                 delim=delim,quote="")
        if(!is.null(selvars)) dat <- dat |> dplyr::select(dplyr::all_of(selvars))
        if(!db) duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=ow,append=append)
        nrec <- dat |> nrow()
        cat(paste0(basename(filename),": ",nrec," records loaded\n"))
        rm(dat)
        gc()
      }else{
        cat(paste0(tab_name," exists\n"))
      }
    }else{
      cat(paste0(filename," not found\n"))
    }
  }else{
    if(!is.null(dataset)){
      if(!tab_name%in%duckdb::dbListTables(dbi) || ow || append){
        if(ow){
          DBI::dbExecute(dbi,paste0("DROP TABLE IF EXISTS ",tab_name))
        }
        dat <- dataset
        names(dataset) <- tolower(names(dataset))
        if(!is.null(selvars)) dat <- dat |> dplyr::select(dplyr::all_of(selvars))
        if(!db) duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=F,append=T)
        nrec <- dat |> nrow()
        cat(paste0(tab_name,": ",nrec," records loaded\n"))
        rm(dat)
      }
    }else{
      cat(paste("Nothing to load\n"))
    }
  }

  if (own_con) duckdb::dbDisconnect(dbi, shutdown = TRUE)

  return(cat(paste0(nrec," records processed\n")))
}
