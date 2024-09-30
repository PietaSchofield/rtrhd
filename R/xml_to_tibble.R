#' xml to tibble
#'
#' @export
xml_to_tibble <- function(fileName,root,wks=3,db=F){

  if(db){
    db <- T
    filePath <- file.path(Sys.getenv("HOME"),"Projects","refdata","dmplusd","4230924_files")
    files <- list.files(filePath,pattern=".*xml",recur=T,full=T)
    root <- "VMPP"
    wks <- 19
  }
  xml_data <- xml2::read_xml(fileName)
  root_nodes <- xml2::xml_find_all(xml_data,paste0(".//",root))
  future::plan(future::multicore,workers=wks) 
  nodes_table <- furrr::future_map(root_nodes,parse_node,root) %>% bind_rows()
  nodes_table
}

#' @export
parse_node <- function(node,idf){
  key_val <- xml2::xml_text(xml2::xml_find_first(node,paste0(".//",idf)))
  children <- xml2::xml_children(node)
  node_names <- setNames(as.list(xml2::xml_text(children)),xml2::xml_name(children)) %>% unlist() 
  tibble::as_tibble(t(node_names))
}
