#' summarise and event table by periods
#'
#' @export
summarise_period <- function(dbf,tabstub=NULL,patienttab,eventtab,idate,edate,patfilter,
                             perstart=NULL,perend=NULL,perdiv="months",prior=1,db=F){
  if(db){
    db <- T
    dbf <- sdb
    tabstub <- "temp_period_data"
    tabstub <- NULL
    patienttab <- "cohort"
    eventtab <- "consultations"
    patfilter <- "(p.condition ILIKE 'SLE' AND p.status ILIKE 'child')"
    perstart <- 6
    perend <- 12
    perdiv <- "months"
    prior <- 1
    idate <- "index_date"
    edate <- "consdate"
  }
  if(prior>0){
    persyn <- "prior"
    if(is.null(perstart)){
      timeintfield <- sprintf("'%s_over_%2.2d'",persyn,perend)
      perfilter <- paste0("(CAST(e.",edate," AS DATE) <= CAST(p.",idate," AS DATE) - INTERVAL '",
        paste(perend,perdiv),"')")
    }else{
      timeintfield <- sprintf("'%s_%2.2d_%2.2d'",persyn,perstart,perend)
      perfilter <- paste0("(CAST(e.",edate," AS DATE) <= CAST(p.",idate," AS DATE) - INTERVAL '",
        paste(perend,perdiv),"' AND\n  CAST(e.",edate," AS DATE) > CAST(p.",idate," AS DATE) ",
        "- INTERVAL '", paste(perstart,perdiv),"')") 
    }
  }else{
    persyn <- "post"
    if(is.null(perend)){
      timeintfield <- sprintf("'%s_over_%2.2d'",persyn,perend)
      perfilter <- paste0("(CAST(e.",edate," AS DATE) <= CAST(p.",idate," AS DATE) + INTERVAL '",
        paste(perend,perdiv),"')")
    }else{
      timeintfield <- sprintf("'%s_%2.2d_%2.2d'",persyn,perstart,perend)
      perfilter <- paste0("(CAST(e.",edate," AS DATE) <= CAST(p.",idate," AS DATE) + INTERVAL '",
        paste(perend,perdiv),"' AND\n CAST(e.",edate," AS DATE) > CAST(p.",idate," AS DATE) + ",
        "INTERVAL '", paste(perstart,perdiv),"')") 
    }
  }
  if(!is.null(tabstub)){
    selectstat <- paste0("CREATE TABLE IF NOT EXISTS ",tabstub," AS SELECT ")
  }else{
    selectstat <- "SELECT DISTINCT "
  }
  unnamed_sql <- rtrhd::tidy_sql(str_c(selectstat,"
      p.patid,
     ",timeintfield," AS timeint,
      COUNT(e.",edate,") AS freq
    FROM 
      ",patienttab," AS p
    INNER JOIN
      ",eventtab," AS e
      ON e.patid=p.patid,
    WHERE 
     ",perfilter," AND
     ",patfilter,"
    GROUP BY 
      p.patid
  "))
  if(db) writeLines(unnamed_sql)
  if(!is.null(tabstub)){
    rtrhd::sql_execute(dbf=dbf,sql_make=unnamed_sql)
    return(unnamed_sql)
  }else{
    res <- rtrhd::get_table(dbf=dbf,sqlstr=unnamed_sql)
    return(res)
  }
}
