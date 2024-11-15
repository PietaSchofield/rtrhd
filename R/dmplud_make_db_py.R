#' make a duckdb 
#'
#' @export
dmplusd_make_db_py <- function(filePath,dbPath=dirname(filePath),dbName=NULL,wks=3,db=F,ow=F){
  if(db){
    db <- T
    filePath <- dmplusddir
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
      vtm=c(filename=list.files(dmdxmldir,pattern="f_vtm",full=T), 
            rows="VTMS",entity="VTM",tabname="vtm",ds="VIRTUAL_THERAPEUTIC_MOIETIES"),
      bnf=c(filename=list.files(dmdxmldir,pattern="f_bnf",full=T,recur=T),
            rows="VMPS",entity="VMP",tabname="bnf",ds="BNF_DETAILS"),
      vmp=c(filename=list.files(dmdxmldir,pattern="f_vmp[0-9]",full=T),
            rows="VMPS",entity="VMP",tabname="vmp",ds="VIRTUAL_MED_PRODUCTS"),
      amp=c(filename=list.files(dmdxmldir,pattern="f_amp[0-9]",full=T),
            rows="AMPS",entity="AMP",tabname="amp"),
      vmpp=c(filename=list.files(dmdxmldir,pattern="f_vmpp",full=T),
            rows="VMPPS",entity="VMPP",tabname="vmpp"),
      ampp=c(filename=list.files(dmdxmldir,pattern="f_ampp",full=T),
            rows="AMPPS",entity="AMPP",tabname="ampp"),
      ingredients=c(filename=list.files(dmdxmldir,pattern="f_ingre",full=T),
            rows="INGS",entity="ING",tabname="ing"),
      lookups=c(filename=list.files(dmdxmldir,pattern="f_lookup",full=T),
            rows="INFOS",entity="INFO",tabname="info"))
    
    if(db){
      reticulate::source_python(system.file('python','debug_keys.py',package='rtrhd'))
      lapply(dmplusd,function(xml){
         xmlf <- xml["filename"]
         csvf <- gsub("xml","csv",xml)
         rows <- xml["rows"]
         rowname <- xml["entity"]
         debug_keys(xmlf)
      })
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
