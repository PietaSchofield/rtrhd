#' Load All Files In Dictionary
#'
#' @export
load_with_data_dictionary <- function(ddFile,dbf,datadir,protocol=NULL,dset,reppref=F,ow=F,db=F){
  if(F){ 
    dset <- "hesopa"
    reppref <- T
    ow <- F
    db <- T
    dbf <- vdb
    ddFile <- opa_dd
    datadir <- linkage_dir
  }
  sheetlist <- rtrhd::read_excel_sheets_to_list(ddFile)
  res <- lapply(names(sheetlist),function(fn){
    rtrhd::load_file_with_data_dictionary(fname=fn,ddict=sheetlist[[fn]],dset=dset,reppref=reppref,
                     ddir=datadir,dbf=dbf,proto=protocol,ow=ow,db=db)
  })
}

#' Construct a col_type parameter from the data-dictionary to pass to the readr::read_tsv function
#'
#' @export
construct_col_types <- function(data_dict) {
  names(data_dict) <- c("name","description","type","format")
  # Mapping data dictionary types to readr col_types
  type_mapping <- list(
    "CHAR" = col_character(),
    "NUMERIC" = col_double(),
    "INTEGER" = col_integer(),
    "LOGICAL" = col_logical(),
    "DATE" =  function(format) col_date(format=format)
  )

  # Create col_types for read_tsv
  col_types <- list()
  for (i in seq_len(nrow(data_dict))) {
    col_name <- data_dict$name[i]
    col_type <- data_dict$type[i]
    col_format <- data_dict$format[i]

    # Handle DATE format dynamically
    if (col_type == "DATE" && !is.na(col_format)) {
      col_types[[col_name]] <- type_mapping[["DATE"]](col_format)
    } else if (!is.null(type_mapping[[col_type]])) {
      col_types[[col_name]] <- type_mapping[[col_type]]
    }
  }
  cols(.default = col_skip(), !!!col_types)
}

#'  Find all files that match the sheet name pattern and read and load them to the database
#'
#' @export
load_file_with_data_dictionary <- function(fname, ddict,ddir,dbf,proto,dset,reppref=F,ow=F,db=F) {
  if(F){
    fname <- names(sheetlist)[3]
    ddict <- sheetlist[[fname]]
    proto <- protocol
    ddir <- datadir
  }
  # Construct col_types using the data dictionary
  fname <- gsub("[.].*","",fname)
  if(reppref){
    tname <- tolower(paste0(dset,"_",gsub("^[^_]*_","",fname)))
  }else{
    tname <- tolower(paste0(dset,"_",fname))
  }

  dbc <- duckdb::dbConnect(duckdb::duckdb(), dbf, write=F)
  tabs <- DBI::dbListTables(dbc)
  duckdb::dbDisconnect(dbc,shutdown=T)
  if(!tname %in% rtrhd::list_tables(dbf=dbf)||ow){
    if(ow){
      rtrhd::sql_execute(dbf=dbf,sql_make=paste0("DROP TABLE IF EXISTS ",tname,";"))
    }
    col_types <- rtrhd::construct_col_types(data_dict=ddict)

    all_files <- list.files(path = ddir, pattern=paste0(".*",fname), full.names = TRUE, recursive = TRUE)
    pattern <- rtrhd::construct_pattern(stub=fname,protocol=proto)
    fpath <- all_files[grepl(pattern,basename(all_files),perl=T)]
    res <- lapply(fpath,function(fn){
      # Read the file with read_tsv
      dat <- read_tsv(fn,col_types = col_types)
      if(db){ 
        cat(paste0(tname,":",paste0(problems(dat),collapse=" , "),"\n"))  
      }
      names(dat) <- tolower(gsub("[.]","_",make.names(names(dat),unique=T)))
      if(nrow(dat)>0){
        dbc <- duckdb::dbConnect(duckdb::duckdb(), dbf, write=T)
        DBI::dbWriteTable(dbc,tname,dat,append=T,overwrite=F)
        duckdb::dbDisconnect(dbc,shutdown=T)
      }
      nrow(dat)
    })
    logger::log_info("{tname}: {sum(unlist(res))} records loaded")
  }
}

#' construct pattern
#'
#' @export
construct_pattern <- function(stub, protocol) {
  if(is.null(protocol)){
    paste0(".*_",stub,"_.*.txt$")
  }else{
    if (grepl("_pathway$", stub)) {
      # Stub explicitly includes "_pathway"
      paste0("^", stub, "(_\\d{4})?_", protocol, "\\.txt$")
    } else {
      # Stub does not include "_pathway"; ensure "_pathway" does not appear
      paste0("^", stub, "(?!_pathway)(_\\d{4})?_", protocol, "\\.txt$")
    }
  }
}

