#' make a duckdb 
#'
#' @export
dmplusd_make_db_r <- function(filePath,dbPath=dirname(filePath),dbName=NULL,wks=3,db=F,ow=F){

  if(db){
    db <- T
    filePath <- dmdpath
    dbPath <- dirname(filePath)
    wks <- 15
    ow <- T
  }

  xmlpath <- file.path(filePath,"xml")
  ver <- gsub("(^f_vtm2_|[.]xml$)","", list.files(xmlpath,pattern="f_vtm")[1])
  if(is.null(dbName)){
    dbName <- file.path(dbPath,paste0("dmplusd_",ver,".duckdb"))
  }
  if(!file.exists(dbName)| ow){
    tabs <- rtrhd::list_tables(dbf=dbName)

    dmdxmldir <- xmlpath
    dmplusd <- list(
      vtm=c(filename=list.files(dmdxmldir,pattern="f_vtm",full=T),entity="VTM",tabname="vtm"),
      bnf=c(filename=list.files(dmdxmldir,pattern="f_bnf",full=T,recur=T),entity="VMP",tabname="bnf"),
      vmp=c(filename=list.files(dmdxmldir,pattern="f_vmp[0-9]",full=T),entity="VMP",tabname="vmp"),
      amp=c(filename=list.files(dmdxmldir,pattern="f_amp[0-9]",full=T),entity="AMP",tabname="amp"),
      vmpp=c(filename=list.files(dmdxmldir,pattern="f_vmpp",full=T),entity="VMPP",tabname="vmpp"),
      ampp=c(filename=list.files(dmdxmldir,pattern="f_ampp",full=T),entity="AMPP",tabname="ampp"),
      ingredients=c(filename=list.files(dmdxmldir,pattern="f_ingre",full=T),entity="ING",tabname="ing"),
      lookups=c(filename=list.files(dmdxmldir,pattern="f_lookup",full=T),entity="INFO",tabname="info"))
    
    if(db){
      dmplusd
    }

    lapply(dmplusd,function(dmd){
      if(file.exists(dmd["filename"])){
        dnd <- rtrhd::xml_to_tibble(fileName=dmd["filename"],root=dmd["entity"],wks=wks) 
        rtrhd::load_table(dbf=dbName,dataset=dnd,tab_name=dmd["tabname"])
      }
    })
  }
  return(dbName)
}
