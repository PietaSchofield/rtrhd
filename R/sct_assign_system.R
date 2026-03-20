#' SNOMED System Classifier
#'
#' Assign one or more body‑system labels to a term
#'
#' @param term A character string (the concept label or synonym)
#' @return A character string containing the matching system(s) separated by "|",
#'         or "Vague" if no system matches
#' @export
assign_system <- function(term) {
  # Normalise the term for case‑insensitive matching

  term <- str_to_lower(term)

  # Define regular‑expression patterns for each body‑system
  sys_patterns <- list(
    Urinary = paste0("urine|urina|cystitis|\\buti\\b|bladder|\\bmsu\\b|urition|urol|",
                         "vesicoureterici|kidney"),
    Skin = paste0("skin|rash|eczema|dermat|wart|psoriasis|cellulitis|abscess|",
                         "rosacea|morphoea|plastici|urticaria|impetigo"),
    MSK = paste0("neck|fracture|sprain|joint|knee|ankle|shoulder|back|muscul|groin|foot|",
                          "tendon|ligament|ortho|lumbar|gait|rib|pain|achilles|limb|sublux|shin|",
                          "spine|muscl|carpal|cramp|elbow|arthr|bursitis|fasci|tendin|finger|bone|",
                          "thumb|wrist|clavic|hand|physiot|chiropod|walking|synovit|rotat|",
                          "coccyx|\\btoe(s)\\b|chondr|patella|enthes|\\bhip(s)\\bi|planus"),
    Respiratory = paste0("cough|asthma|respiratory|resp|lung|wheez|bronch|sinus|chest|breath|",
                                 "soboe|flow rate|rhonchi"),
    Gynaecological = paste0("uterus|pregnan|gynae|cervix|menorrhoea|obste|uterin|",
                                 "rrhagia|natal|aborti|ovari|vagin|vulva"),
    Genital = paste0("vagin|vulva|penis|balanoposthitis|scrota"),
    STI = paste0("chlamydia|gonorrhoea|\\uti\\b"),
    Lymphatic = paste0("adenitis|lymph"),
    Blood = paste0("haemoglobin|anaemia|fbc|blood|packed cell"),
    Coagulation = paste0("thromboplastin|coagulation|inr|aptt|bruis|haematom"),
    Liver = paste0("liver|\\balt\\b|bilirubin"),
    Endocrine = paste0("glucose|thyroid|hormone|diabetes|endocri|gland"),
    Mental = paste0("anxi|depressi|mental|behaviour|emotion|crying|mood|stress|",
                                 "psychia"),
    Aural = paste0("\\bear\\b|hearing|audio|otitis|tympanic|eustachian|audito"),
    Ocular = paste0("eye|vision|fundoscopy|conjunctivitis"),
    Gastro = paste0("abdom|bowel|stool|faeces|gastro|vomit|diarrhoea|iliac fossa|",
                           "constipation|digesti|gastri"),
    Cardio = paste0("heart|bp|cardio|ecg|pulse"),
    Vascular = paste0("varicose|vein|raynaud"),
    Infection = paste0("virus|viral|serology|mononucleosis|hepatitis|infect|mantoux|iasis|herpes"),
    Weight = paste0("weight|bmi|obesity|body mass"),
    Neuro = paste0("neuro|migraine|seizure|epilepsy|falls|conscious|convul|nervo|",
                           "sleep|numb|incoordina"),
    Oral = paste0("throat|tonsil|pharyn|mouth|\\bent\\b|dental|halito|bad breath|",
                           "oral|oesophag|teeth|layrng|trachei"),
    Nasal = paste0("nasal|sneez|rhinit"),
    Occular = paste0("blepharitis"),
    Wound = paste0("wound|scar"),
    Fatigue = paste0("fatigue|tired|neurasth"),
    Adbominal = paste0("pelvi|abdom|iliac fossa"),
    Pain = paste0("pain|migraine|algia"),
    Kidney = paste0("urine|urina|urition|urol|vesicoureterici|kidney")
  )

  split_patterns <- lapply(sys_patterns,function(x) strsplit(x,"[|]")[[1]]) 

  pattern_df <- dplyr::bind_rows(
    lapply(names(split_patterns), function(x){
       data.frame(
          stub = split_patterns[[x]],
          thought = x,
          stringsAsFactors = FALSE
        )
      })
    )

  if(F){
    pattern_df |> dplyr::arrange(stub) |> 
      readr::write_csv(file.path(Sys.getenv("HOME"),"Projects","sprint","refs",
                                 "system_pattern_classes.csv"))
  }

  # Find which system patterns match the term
  match_idx <- sapply(sys_patterns, function(pat) str_detect(term, pat))

  if(length(names(which(match_idx))) >= 1){
    system_map <- paste0(names(which(match_idx[1:length(match_idx)]),collapse="|"))
  }else{
    system_map <- "Vague"
  }


  # Otherwise, return the matching system names concatenated with "|"
  return(system_map)
}

