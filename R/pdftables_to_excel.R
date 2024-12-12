#' pdftables_to_excel
#'
#' @export
pdftables_to_excel <- function(fileName,outFile,silent=F){
  if(!reticulate::py_module_available("pdfplumber")){
    reticulate::py_install("pdfplumber")
  }
  if(!reticulate::py_module_available("xlsxwriter")){
    reticulate::py_install("xlsxwriter")
  }

  reticulate::source_python(system.file("python","extract_tables_to_excel.py",package="rtrhd"))

  tables <- extract_tables_to_excel(fileName,outFile)
  if(silent){
    return(outFile)
  }else{
    return(tables)
  }
}
