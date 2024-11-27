#' generate set of regular periods
#'
#' @export
generate_periods <- function(months_before, months_after, period_length,aslist=T) {
  periods <- list()
  # Generate periods prior to the index date
  if (months_before > 0) {
    for (i in seq(months_before, period_length, by = -period_length)) {
      pstart <- max(i, 0)
      pend <- max(i - period_length, 0)
      periods[[sprintf("p%2.2d", length(periods) + 1)]] <- 
        list(pstart = pstart, pend = pend, prior = 1,label=sprintf("prior_%2.2d_%2.2d",pstart,pend))
    }
  }
  # Generate periods after the index date
  if (months_after > 0) {
    for (i in seq(0, months_after - period_length, by = period_length)) {
      pstart <- i
      pend <- i + period_length
      periods[[sprintf("p%2.2d", length(periods) + 1)]] <- 
        list(pstart = pstart, pend = pend, prior = 0,label=sprintf("post_%2.2d_%2.2d",pstart,pend))
    }
  }
  if(aslist){
    return(periods)
  }else{
    return(lapply(periods,unlist) %>% plyr::ldply(.id="period"))
  }
}

