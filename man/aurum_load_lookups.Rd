% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/aurum_load_lookups.R
\name{aurum_load_lookups}
\alias{aurum_load_lookups}
\title{Load the aurum lookups files into a database}
\usage{
aurum_load_lookups(txtdir, dbf, ow = F, db = F, silent = T)
}
\arguments{
\item{bdir}{the name of the batch subdirectory}

\item{odir}{the name of the output directory}

\item{olist}{the list of observation codes}

\item{bpp}{BiocParallal Multicore Parameters

Pass a table of covariate codes and generate covariates table}
}
\description{
get the drugissue records and convert some fields to useful field types
}
