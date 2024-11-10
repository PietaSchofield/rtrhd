#' Generate Sankey Network Data
#'
#' This is necessary because of the limitations on SQL Statements to duckdb
#' @export
gen_sankey_data <- function(dbf, condition='SLE',status='child', tabstub=paste0('temp_',condition),
                            tabname=paste0(condition,"_sankey"),db=F){
  if(db){
    dbf <- adb 
    condition <- 'SLE'
    status <- 'child'
    tabstub <- paste0('temp_',condition)
    tabname <- paste0(condition,"_sankey")
  }

  temp_tabs <- rtrhd::list_tables(dbf)
  temp_tabs <- temp_tabs[grepl("^temp_",temp_tabs)]
  res <- lapply(temp_tabs,function(tn){
    rtrhd::sql_execute(dbf=dbf,sql_make=paste0("DROP TABLE ",tn,";"))
  })


  ## Over 36 Months prior
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'bo36')," AS 
  SELECT
    p.patid,
    'prior_over36' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) <= CAST(p.index_date AS DATE) - INTERVAL '36 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  
  ## 18-36 prior
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'b18')," AS 
  SELECT
    p.patid,
    'prior_18to36' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) > CAST(p.index_date AS DATE) - INTERVAL '36 months' AND
    CAST(c.consdate AS DATE) <= CAST(p.index_date AS DATE) - INTERVAL '18 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## 6-18 prior
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'b06')," AS 
  SELECT
    p.patid,
    'prior_06to18' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) > CAST(p.index_date AS DATE) - INTERVAL '18 months' AND
    CAST(c.consdate AS DATE) <= CAST(p.index_date AS DATE) - INTERVAL '6 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## 3-6 prior
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'b00')," AS 
  SELECT
    p.patid,
    'prior_00to06' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) > CAST(p.index_date AS DATE) - INTERVAL '6 months' AND
    CAST(c.consdate AS DATE) <= CAST(p.index_date AS DATE) - INTERVAL '0 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## 0-6 post
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'a00')," AS 
  SELECT
    p.patid,
    'post_00to06' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) >= CAST(p.index_date AS DATE) + INTERVAL '0 months' AND
    CAST(c.consdate AS DATE) < CAST(p.index_date AS DATE) + INTERVAL '6 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## 6-18 post
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'a06')," AS 
  SELECT
    p.patid,
    'post_06to18' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) >= CAST(p.index_date AS DATE) + INTERVAL '6 months' AND
    CAST(c.consdate AS DATE) < CAST(p.index_date AS DATE) + INTERVAL '18 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## 18-36 post
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'a18')," AS 
  SELECT
    p.patid,
    'post_18to36' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) >= CAST(p.index_date AS DATE) + INTERVAL '18 months' AND
    CAST(c.consdate AS DATE) < CAST(p.index_date AS DATE) + INTERVAL '36 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## over36 post
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0(tabstub,'a36')," AS 
  SELECT
    p.patid,
    'post_over36' AS timeint,
    COUNT(consid) AS freq
  FROM 
    cohort AS p
  INNER JOIN
    consultations AS c
    ON c.patid=p.patid,
  WHERE 
    CAST(c.consdate AS DATE) >= CAST(p.index_date AS DATE) + INTERVAL '36 months' AND
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"'
  GROUP BY 
    p.patid
  ")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  ## START SQL BLOCK
  make_sql <- str_c("CREATE TABLE IF NOT EXISTS ",paste0('temp_',tabname)," AS (
  SELECT * FROM ",paste0(tabstub,'bo36'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'b18'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'b06'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'b00'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'a00'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'a06'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'a18'),"
  UNION ALL
  SELECT * FROM ",paste0(tabstub,'a36'),");")
  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)


  timeint_sql <- str_c("SELECT DISTINCT
    timeint
  FROM
    ",paste0('temp_',tabname),";")

  time_levels <- rtrhd::get_table(dbf=dbf,sqlstr=timeint_sql) %>% pull(timeint)

  make_sql <- str_c("DROP TABLE IF EXISTS temp_intervals;
  CREATE TABLE IF NOT EXISTS temp_intervals ( 
    timeint NVARCHAR(15)
  );

  INSERT INTO temp_intervals (timeint)
    VALUES('prior_over36'),('prior_18to36'),('prior_06to18'),('prior_00to06'),
          ('post_00to06'),('post_06to18'),('post_18to36'),('post_over36');")

  res <- rtrhd::sql_execute(dbf=dbf,sql_make=make_sql)

  unnamed_sql <- str_c("WITH all_patient_intervals AS(
  SELECT DISTINCT
    p.patid,
    p.condition,
    p.status,
    i.timeint
  FROM cohort AS p
  CROSS JOIN temp_intervals AS i
  WHERE 
    p.condition ILIKE '",condition,"' AND
    p.status ILIKE '",status,"')

  SELECT DISTINCT
    api.patid,
    api.timeint,
    COALESCE(pi.freq, 0) AS freq
  FROM 
    all_patient_intervals AS api
  LEFT JOIN
    ",paste0('temp_',tabname)," AS pi
    ON api.patid=pi.patid AND api.timeint=pi.timeint 
  WHERE 
    api.condition ILIKE '",condition,"' AND
    api.status ILIKE '",status,"' ")

  res <- rtrhd::get_table(dbf=dbf,sqlstr=unnamed_sql)

  time_levels <- c('prior_over36','prior_18to36','prior_06to18','prior_00to06',
          'post_00to06','post_06to18','post_18to36','post_over36')
  
  freq_levels <- c("none","low","mod","high")
  
  res$timeint <- factor(res$timeint,levels=time_levels)

  res$freqband <- cut(res$freq,breaks=c(-Inf,0,3,6,Inf),labels=freq_levels)
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

  nodes_df

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
