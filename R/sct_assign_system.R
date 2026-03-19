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
    Urinary = stringr::regex(paste0("urine|urina|cystitis|uti|bladder|msu|urition|urol|",
                         "vesicoureterici|kidney")),
    Skin = stringr::regex(paste0("skin|rash|eczema|dermat|wart|psoriasis|cellulitis|abscess|",
                         "rosacea|morphoea|plastici|urticaria|impetigo")),
    MSK = stringr::regex(paste0("neck|fracture|sprain|joint|knee|ankle|shoulder|back|muscul|groin|foot|",
                          "tendon|ligament|ortho|lumbar|gait|rib|pain|achilles|limb|sublux|shin|",
                          "spine|muscl|carpal|cramp|elbow|arthr|bursitis|fasci|tendin|finger|bone|",
                          "thumb|wrist|clavic|hand|physiot|chiropod|walking|synovit|rotat|",
                          "coccyx|\\btoe(s)\\b|chondr|patella|enthes|\\bhip(s)\\bi|planus")),
    Respiratory = stringr::regex(paste0("cough|asthma|respiratory|resp|lung|wheez|bronch|sinus|chest|breath|",
                                 "soboe|flow rate|rhonchi")),
    Gynaecological = stringr::regex(paste0("uterus|pregnan|gynae|cervix|menorrhoea|obste|uterin|",
                                 "rrhagia|natal|aborti|ovari")),
    Genital = stringr::regex(paste0("vagin|vulva|penis|balanoposthitis")),
    STI = stringr::regex(paste0("chlamydia|gonorrhoea")),
    Lymphatic = stringr::regex(paste0("adenitis")),
    Blood = stringr::regex(paste0("haemoglobin|anaemia|fbc|blood|packed cell")),
    Coagulation = stringr::regex(paste0("thromboplastin|coagulation|inr|aptt|bruis|haematom")),
    Liver = stringr::regex(paste0("liver|alt|bilirubin")),
    Endocrine = stringr::regex(paste0("glucose|thyroid|hormone|diabetes|endocri|gland")),
    Mental = stringr::regex(paste0("anxi|depressi|mental|behaviour|emotion|crying|mood|stress|",
                                 "psychia")),
    Aural = stringr::regex(paste0("ear|hearing|audio|otitis|tympanic|eustachian|audito")),
    Ocular = stringr::regex(paste0("eye|vision|fundoscopy|conjunctivitis")),
    Gastro = stringr::regex(paste0("abdom|bowel|stool|faeces|gastro|vomit|diarrhoea|iliac fossa|",
                           "constipation|digesti|gastri")),
    Cardio = stringr::regex(paste0("heart|bp|cardio|ecg|pulse")),
    Vascular = stringr::regex(paste0("varicose|vein|raynaud")),
    Infection = stringr::regex(paste0("virus|viral|serology|mononucleosis|hepatitis")),
    Weight = stringr::regex(paste0("weight|bmi|obesity|body mass")),
    Neuro = stringr::regex(paste0("neuro|migraine|seizure|epilepsy|falls|conscious|convul|nervo|",
                           "sleep|numb|incoordina")),
    Oral = stringr::regex(paste0("throat|tonsil|pharyn|mouth|\\bent\\b|dental|halito|bad breath|",
                           "oral|oesophag|teeth|layrng|trachei")),
    Nasal = stringr::regex(paste0("nasal|sneez|rhinit")),
    Occular = stringr::regex(paste0("blepharitis")),
    Wound = stringr::regex(paste0("wound|scar")),
    Infection2 = stringr::regex(paste0("infect|mantoux|iasis|herpes")),
    Fatigue = stringr::regex(paste0("fatigue|tired|neurasth")),
    Adbominal = stringr::regex(paste0("pelvi|abdom")),
    Pain = stringr::regex(paste0("pain|migraine|algia")),
    Kidney = stringr::regex(paste0("urine|urina|urition|urol|vesicoureterici|kidney"))
  )

  if(F){
    rtrhd::export_system_patterns(sys_patterns,
        file.path(Sys.getenv("HOME"),"Projects","sprint","refs","system_patterns.csv"))
  }

  # Find which system patterns match the term
  match_idx <- sapply(sys_patterns, function(pat) str_detect(term, pat))

  if(length(names(which(match_idx))) >= 1){
    system_map <- paste0(names(which(match_idx[1:length(match_idx)])),collapse="|")
  }else{
    system_map <- "Vague"
  }


  # Otherwise, return the matching system names concatenated with "|"
  return(system_map)
}

