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
make_control_aurum_ab <- function(dbn, exclude, include, reps=1,wks=7,
                                  agebin=0,  silent=F, tab_name="sample_cohort") {
  if (F) {
    dbn <- dbFile
    agebin <- 5
    silent <- T
    reps <- 8
    exclude <- exposed
    include <- unexposed
    tab_name <- "sample_cohort_ab"
    wks <- 15
  }

  future::plan(future::multisession,workers=wks)

  # Load exclude and include datasets into the database
  ret <- rtrhd::load_table(dbf = dbn, dataset = exclude, tab_name = "exclude", ow = T)
  ret <- rtrhd::load_table(dbf = dbn, dataset = include, tab_name = "include", ow = T)

  # Query for all age-sex-practice combinations in the cases
  sample_plan_sql <- glue::glue("
    SELECT DISTINCT
      prac.pracid,
      pat.yob,
      pat.gender,
      prac.region, 
      COUNT(*) AS cases
    FROM
      acceptable_patients AS pat
    INNER JOIN
      exclude AS ic
      ON ic.patid = pat.patid
    INNER JOIN
      practices AS prac
      ON prac.pracid = pat.pracid
    WHERE
      pat.yob IS NOT NULL AND pat.gender IS NOT NULL
    GROUP BY
      prac.region, prac.pracid, pat.yob, pat.gender;
  ")
  sample_plan <- rtrhd::get_table(dbf = dbn, sqlstr = sample_plan_sql)

  # Query for potential controls (initial set)
  get_possibles_sql <- glue::glue("
    SELECT DISTINCT
      pat.patid,
      pra.pracid,
      pat.yob,
      pat.gender,
      pra.region
    FROM
      acceptable_patients AS pat
    INNER JOIN
      practices AS pra
      ON pra.pracid = pat.pracid
    LEFT JOIN
      exclude AS exc
      ON exc.patid = pat.patid
    INNER JOIN
      include AS inc
      ON inc.patid = pat.patid
    WHERE
      exc.patid IS NULL AND
      pat.yob IS NOT NULL AND pat.gender IS NOT NULL;
  ")
  possibles <- rtrhd::get_table(dbf = dbn, sqlstr = get_possibles_sql)

  # Function to sample controls dynamically
  dynamic_sampling_prac <- function(prac, yob, gender, cases, possibles, reps, agebin) {
    # Filter possibles dynamically for the current combination
    subset_possibles <- possibles %>%
      dplyr::filter(
        pracid == prac,
        gender == gender,
        abs(yob - yob) <= agebin
      )

    # Sample the required number of controls
    sampled <- subset_possibles %>%
      slice_sample(n = cases * reps, replace = FALSE)

    return(sampled)
  }

  # Iterate over age-sex-practice combinations and collect controls
  practice_matches <- sample_plan %>%
    furrr::future_pmap_dfr(
      .f = ~ dynamic_sampling_prac(..1, ..2, ..3, ..5, possibles, reps, agebin),
     .options = furrr::furrr_options(seed=T)
     )
  
  unmatched_groups <- sample_plan %>%
    anti_join(practice_matches, by = c("pracid", "yob", "gender")) %>%
    group_by(yob,gender,region) %>% summarise(cases=sum(cases),.groups="drop")

  # Function to sample controls dynamically
  dynamic_sampling_region <- function(yob, gender,region, cases, possibles, reps, agebin) {
    # Filter possibles dynamically for the current combination
    subset_possibles <- possibles %>%
      dplyr::filter(
        region == region,
        gender == gender,
        abs(yob - yob) <= agebin
      )

    # Sample the required number of controls
    sampled <- subset_possibles %>%
      slice_sample(n = reps, replace = FALSE)

    return(sampled)
  }

  # Step 2: Region-level matching for unmatched groups
  if (nrow(unmatched_groups) > 0) {
    region_matches <- unmatched_groups %>%
      furrr::future_pmap_dfr(
        .f = ~ dynamic_sampling_region(..1, ..2, ..3, ..4, possibles, reps, agebin),
        .options = furrr::furrr_options(seed = TRUE)
      )
  } else {
    region_matches <- tibble()
  }

  unmatched <- unmatched_groups %>%
    anti_join(region_matches, by = c("region","yob","gender")) %>% 
    group_by(yob,gender) %>% summarise(cases=sum(cases),.groups="drop") 


  # Get case data
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
      ON ic.patid = pat.patid
    INNER JOIN
      practices AS pra
      ON pra.pracid = pat.pracid
  ")
  cases_data <- rtrhd::get_table(dbf = dbn, sqlstr = cases_sql)

  # Combine cases and controls
  all_patients <- list(case = cases_data, 
                       practice_matched = practice_matches, 
                       region_matched=region_matches) %>%
    plyr::ldply(.id = "cohort") %>%
    tibble()


  # Save the cohort to the database
  rtrhd::load_table(dbf = dbn, dataset = all_patients, tab_name = tab_name, ow = T)
  rtrhd::load_table(dbf = dbn, dataset = unmatched, tab_name = paste0(tab_name,"_unmatched"),ow=T)

  return(nrow(all_patients))
}

