% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/make_controls_aurum_ab.R
\name{make_control_aurum_ab}
\alias{make_control_aurum_ab}
\title{Make a control cohort using some inclusion and exclusion lists}
\usage{
make_control_aurum_ab(
  dbn,
  exclude,
  include,
  reps = 1,
  wks = 7,
  agebin = 0,
  silent = F,
  tab_name = "sample_cohort"
)
}
\arguments{
\item{exclude}{is a list of patients ids that are in the cases so wont be in the controls}

\item{include}{is a list of patients who are possibles ie don't include the exclude list but satify
other criteria also}

\item{reps}{is the number of controls per case}

\item{agebin}{is how many years either side the age matching is done}

\item{praclevel}{is either region or pracid}
}
\description{
Perhaps rather counter intuitively I called the cases the exclude list as they are to be excluded from
the controls, I suppose was my logic, the patients to include in the controls are called the include
list which is perhaps the source of my confusion
}
