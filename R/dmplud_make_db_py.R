#' make a duckdb 
#'
#' @export
dmplusd_make_db_py <- function(filePath,dbPath=dirname(filePath),dbName=NULL,
                               xml_config=system.file("xml","dmd_structure.xml",package="rtrhd"),
                                                      db=F,ow=F){
  if(db){
    db <- T
    filePath <- dmplusddir
    dbPath <- dirname(filePath)
    wks <- 15
    ow <- T
    dbName <- ddbFile
  }
  reticulate::source_python(system.file("python","xml_to_dataframe.py",package="rtrhd"))
  file_list <- get_xml_config(xml_config)
  makedb <- lapply(file_list,add_data,filePath,dbName)
  return(dbName)
}

#' @export
get_xml_config <- function(xml_conf){
  xml_config <- xml2::read_xml(xml_conf)

  # Extract all <file> nodes
  file_nodes <- xml2::xml_find_all(xml_config, ".//file")

  # Convert each <file> node into a named list
  file_list <- lapply(file_nodes, function(node) {
    list(
      file_name = xml2::xml_text(xml2::xml_find_first(node, "./file_name")),
      dataset_key = xml2::xml_text(xml2::xml_find_first(node, "./dataset_key")),
      rows_key = xml2::xml_text(xml2::xml_find_first(node, "./rows_key")),
      rowname_key = xml2::xml_text(xml2::xml_find_first(node, "./rowname_key")),
      table_name = xml2::xml_text(xml2::xml_find_first(node, "./table_name"))
    )
  })
  return(file_list)
}

#' @export
get_xml_data <- function(fl,dmddir,db=F){
  if(db){
    fl <- file_list[[1]]
    dmddir <- dmplusddir
  }
  fn <- list.files(file.path(dmddir,"xml"),pattern=fl$file_name,recur=T,full=T)
  dsk <- fl$dataset_key
  rsk <- fl$rows_key
  rnk <- fl$rowname_key
  xml_to_dataframe(fn,dsk,rsk,rnk)  
}

#' @export
add_xml_data <- function(fl,dmddir,dbfile,db=F){
  if(db){
    fl <- file_list[[1]]
    dmddir <- dmplusddir
    dbfile <- ddbFile
  }
  tn <- fl$table_name
  dat <- get_data(fl,dmddir)
  if(nrow(dat)>0){
    rtrhd::load_table(dbf=dbfile,dataset=dat,tab_name=tn,ow=F)
  }
}

