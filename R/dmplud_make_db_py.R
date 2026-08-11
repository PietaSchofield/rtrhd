#' make a duckdb 
#'
#' @export
dmplusd_make_db_py <- function(filePath,dbPath=dirname(filePath),dbName=NULL,
                               xml_config=system.file("xml","dmd_structure.xml",package="rtrhd"),
                                                      db=F,ow=F){
  if(db){
    db <- T
    filePath <- files
    dmddir <- files
    dbPath <- dirname(files)
    wks <- 15
    ow <- T
    dbName <- file.path(dbPath,dbname)
    xml_config <- system.file("xml","dmd_structure.xml",package="rtrhd")
  }
  if(!reticulate::py_module_available("xmltodict")){
    reticulate::py_install("xmltodict")
  }
  reticulate::source_python(system.file("python","xml_to_dataframe.py",package="rtrhd"))
  file_list <- rtrhd::get_xml_config(xml_config)
  makedb <- lapply(file_list,rtrhd::add_xml_data,filePath,dbName)
  return(dbName)
}

#' get_xml_config
#'
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


#' get xml data
#'
#' @export
get_xml_data <- function(fl,dmddir,db=F){
  reticulate::source_python(system.file("python","xml_to_dataframe.py",package="rtrhd"))
  if(db){
    fl <- file_list[[1]]
  }

  if(!dir.exists(dmddir)){
    stop("dmd+d directory not found: '", dmddir, "'. Check filePath/dmddir argument.")
  }

  # find candidates by pattern (recursive - handles xml/ subdir or flat layout)
  fn <- list.files(dmddir, pattern=fl$file_name, recursive=TRUE, full.names=TRUE, ignore.case=TRUE)

  # enforce .xml extension regardless of whether the config pattern did
  fn <- fn[tolower(tools::file_ext(fn)) == "xml"]

  if(length(fn) == 0){
    stop(
      "No .xml file found matching pattern '", fl$file_name, "' ",
      "for table '", fl$table_name, "' under '", dmddir, "' (searched recursively).\n",
      "Check that: (1) the dm+d files have been extracted into this directory, ",
      "(2) the file_name pattern in the xml config matches the actual filename, ",
      "and (3) you're not accidentally matching only the .xsd schema file."
    )
  }

  if(length(fn) > 1){
    stop(
      "Multiple files matched pattern '", fl$file_name, "' ",
      "for table '", fl$table_name, "' under '", dmddir, "':\n",
      paste("  -", fn, collapse="\n"), "\n",
      "The pattern needs to be more specific - only one .xml file should match per table."
    )
  }

  dsk <- fl$dataset_key
  rsk <- fl$rows_key
  rnk <- fl$rowname_key
  xml_to_dataframe(fn,dsk,rsk,rnk)
}

#' add xml data
#'
#' @export
add_xml_data <- function(fl,dmddir,dbfile,db=F){
  if(db){
    fl <- file_list[[1]]
    fl[["file_name"]] <- "f_vtm2.*xml"
    dbfile <- dbname
  }
  tn <- fl$table_name
  dat <- get_xml_data(fl,dmddir)

  if(is.null(dat) || nrow(dat)==0){
    warning("Table '", tn, "' produced 0 rows from source matching '", fl$file_name,
            "' - skipping load_table. Check the XML content/keys (dataset_key='",
            fl$dataset_key, "', rows_key='", fl$rows_key, "').")
    return(invisible(NULL))
  }

  rtrhd::load_table(dbf=dbfile,dataset=dat,tab_name=tn,ow=T)
}

