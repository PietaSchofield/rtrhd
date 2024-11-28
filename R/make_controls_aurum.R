#' Make a control cohort using some inclusion and exclusion lists
#'
#' Perhaps rather counter intuitively I called the cases the exclude list as they are to be excluded from
#' the controls, I suppose was my logic, the patients to include in the controls are called the include
#' list which is perhaps the source of my confusion
#'
#' @param exclude is a list of patients ids that are in the cases so wont be in the controls
#' @param include is a list of patients who are possibles ie don't include the exclude list but satify
#'        other criteria also
#' @param reps is the number of controls per case
#' @param praclevel is either region or pracid 
#' @param agebin is how many years either side the age matching is done
#'
#' @export
make_control_aurum <- function(dbn,exclude,include,reps=1,praclevel=NULL,agebin=0,db=F,silent=F){
  if(db){
    dbn <- dbFile
    praclevel <- "pracid"
    agebin <- 5
    db <- F
    silent <- T
    reps <- 8
    exclude <- exposed 
    include <- unexposed 
  }

  future::plan(future::multisession)
  rtrhd::load_table(dbf=dbn,dataset=exclude,tab_name="exclude",ow=T)
  rtrhd::load_table(dbf=dbn,dataset=include,tab_name="include",ow=T)

  sample_plan_sql <- glue::glue("SELECT DISTINCT
      prac.{praclevel},
      pat.yob,
      pat.gender,
      COUNT(*) AS nums
    FROM
      acceptable_patients AS pat
    INNER JOIN 
      exclude AS ic
      ON ic.patid=pat.patid
    INNER JOIN
      practices AS prac
      ON prac.pracid=pat.pracid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1'
    GROUP BY
      pat.yob,
      pat.gender,
      prac.{praclevel};")
  
  sample_plan <- rtrhd::get_table(dbf=dbn,sqlstr=sample_plan_sql)

  get_possibles_sql <- glue::glue("SELECT DISTINCT
     pat.patid,
     pra.{praclevel},
     pat.yob,
     pat.gender
   FROM 
     acceptable_patients AS pat
   INNER JOIN
     practices AS pra
     ON pra.pracid=pat.pracid
   LEFT JOIN
     exclude AS exc
     ON exc.patid=pat.patid
   INNER JOIN
     include AS inc
     ON inc.patid=pat.patid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     exc.patid IS NULL AND
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1';")

  possibles <- rtrhd::get_table(dbf=dbn,sqlstr=get_possibles_sql)

  sampled_data <- sample_plan %>%
    left_join(possibles, by = c(praclevel,"yob","gender")) %>%
    group_split(across(all_of(c(praclevel,"yob", "gender")))) 

  sampled_data1 <- sampled_data %>% 
    furrr::future_map_dfr(~ {
      ss <- .x %>% pull(nums) %>% unique()
      .x %>% slice_sample(n=reps*ss,replace = FALSE)
     },.options = furrr::furrr_options(seed = TRUE) ) %>% select(-nums)

  missing_data <- sampled_data1 %>% dplyr::filter(is.na(patid)) %>% 
    group_by(yob,gender) %>% summarise(nums=n(),.groups="drop")

  sampled_data <- missing_data %>%
    left_join(possibles, by = c("yob","gender")) %>%
    group_split(yob, gender) 

  sampled_data2 <- lapply(sampled_data,function(x){
    ss <- x %>% pull(nums) %>% unique()
    slice_sample(x, n=reps*ss,replace = FALSE)
     }) %>% bind_rows() %>% select(-nums)

  sampled_data2 %>% dplyr::filter(is.na(patid))
  rm(possibles)
  gc()

  control_data <- bind_rows(sampled_data1,sampled_data2) %>% dplyr::filter(!is.na(patid))
  
  cases_sql <- glue::glue("
    SELECT DISTINCT
      pat.patid,
      pat.gender,
      pat.yob,
      pat.pracid,
      pra.region
    FROM
      acceptable_patients AS pat
    INNER JOIN 
      exclude AS ic
      ON ic.patid=pat.patid
    INNER JOIN
      practices AS pra
      ON pra.pracid=pat.pracid
   INNER JOIN
     linkages AS lnk
     ON lnk.patid=pat.patid
   WHERE
     lnk.hes_apc_e LIKE '1' AND
     lnk.ons_death_e LIKE '1' AND
     lnk.lsoa_e LIKE '1' AND
     lnk.hes_op_e LIKE '1' AND
     lnk.hes_ae_e LIKE '1'")
  
  cases_data <- rtrhd::get_table(dbf=dbn,sqlstr=cases_sql)

  all_patients <- list(case=cases_data,control=control_data) %>% plyr::ldply(.id="cohort") %>% tibble()
  ret <- all_patients %>% nrow()
  rtrhd::load_table(dbf=dbn,dataset=all_patients,tab_name="sample_cohort",ow=T)

  return(ret)
}
