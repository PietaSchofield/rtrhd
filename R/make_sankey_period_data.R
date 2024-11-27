#' Generate Sankey Network Data
#'
#' This is necessary because of the limitations on SQL Statements to duckdb
#' @export
make_sankey_period_data <- function(dbf,perioddata,periods,patfilter,db=F,
    freq_levels=c("none","low","mod","high"), freq_cuts=c(-Inf,0,10,20,Inf),
    pdtab="temp_perioddata",ptab="temp_periods"){
  if(db){
    dbf <- sdb
    perioddata <- datalist 
    patfilter <- patfilter
    periods <- periods
    pdtab <- "temp_perioddata"
    ptab <- "temp_periods"
    freq_levels <- c("none","low","mod","high")
    freq_cuts <- c(-Inf,0,10,20,Inf)
  }

  rtrhd::load_table(dbf=dbf,dataset=perioddata,tab_name=pdtab,ow=T)
  rtrhd::load_table(dbf=dbf,dataset=periods,tab_name=ptab,ow=T)

  time_levels <- periods %>% arrange(period) %>% pull(label)

  unnamed_sql <- rtrhd::tidy_sql(str_c("WITH all_patient_intervals AS(
  SELECT DISTINCT
    p.patid,
    p.condition,
    p.status,
    i.label AS timeint
  FROM cohort AS p
  CROSS JOIN temp_periods AS i
  WHERE 
    ",patfilter,")

  SELECT DISTINCT
    api.patid,
    api.timeint,
    COALESCE(pi.freq, 0) AS freq
  FROM 
    all_patient_intervals AS api
  LEFT JOIN
    ",pdtab," AS pi
    ON api.patid=pi.patid AND api.timeint=pi.timeint;"))

  res <- rtrhd::get_table(dbf=dbf,sqlstr=unnamed_sql)
  
  res$timeint <- factor(res$timeint,levels=time_levels)

  res$freqband <- cut(res$freq,breaks=freq_cuts,labels=freq_levels)
  res$freqband <- factor(res$freqband,levels=freq_levels)

  if(db){
    ressum <- res %>% group_by(timeint,freqband) %>% summarise(pats=n(),.groups="drop")
    ressum %>% as.data.frame()
  }

  restrans <- res %>% arrange(patid,timeint) %>%
    group_by(patid) %>%
    dplyr::mutate(prevband = dplyr::lag(freqband),
                  prevint = dplyr::lag(timeint) ) %>% 
    ungroup() %>% dplyr::filter(!is.na(prevband) & !is.na(prevint))

  restrans$timeint <- factor(restrans$timeint,levels=time_levels)
  restrans$prevint <- factor(restrans$prevint,levels=time_levels)
  restrans$freqband <- factor(restrans$freqband,levels=freq_levels)
  restrans$prevband <- factor(restrans$prevband,levels=freq_levels)

  pattrans <- res %>% select(patid,timeint,freqband) %>% arrange(timeint) %>%
    pivot_wider(names_from="timeint",values_from="freqband",values_fill="low")

  restranssum <- restrans %>% group_by(prevint,prevband,timeint,freqband) %>%
    summarise(pats=n(),.groups="drop")

  # Create unique nodes

  unique_nodes <- unique(c(
    paste(restranssum$prevband, restranssum$prevint, sep = " - "),
    paste(restranssum$freqband, restranssum$timeint, sep = " - ")
  ))

  nodes_df <- data.frame(name_interval = unique_nodes) %>% 
    dplyr::mutate(match=name_interval) %>%
    separate(name_interval, into = c("name","interval"),sep=" - ") %>% 
    dplyr::mutate(order=case_when(
                            grepl("none",name) ~1, 
                            grepl("low",name) ~2, 
                            grepl("mod",name) ~ 3, 
                            grepl("high",name) ~4)) 

  nodes_df$interval <- factor(nodes_df$interval,levels=time_levels)

  nodes_df  <- nodes_df %>% arrange(interval,order)

  # Map nodes to indices for source and target

  restranssum <- restranssum %>%
    mutate(
      source = match(paste(prevband, prevint, sep = " - "), nodes_df$match) - 1,
      target = match(paste(freqband, timeint, sep = " - "), nodes_df$match) - 1
    )
  
  links_df <- data.frame(
    source = restranssum$source,
    target = restranssum$target,
    value = restranssum$pats
  )

  datalist <- list(nodes=nodes_df,links=links_df,timeint=time_levels,trans=pattrans)
  return(datalist)
}
