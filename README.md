R Tools for Routine Healthcare Data
================
Pieta
2024-11-15

# Motivation

I currently have several CPRD project using Aurum, I will also have a
GOLD project soon. There are some common steps that need to be achieved
with these projects.

The idea is that if I can import the data into an
[DuckDB](https://github.com/duckdb/duckdb) database it will permit the
use of SQL to join and extract data for the process of linkage and
cleaning. This also means the original text files need only be visited
once. Some projects have billions of observation records so managing
them in memory in R is not really feasible. DuckDB also has [a well
documented R api](https://duckdb.org/docs/api/r) and some very nifty SQL
dialect aspects that are worth discovering. Its biggest frustration for
me at the moment is that it seems to have a 1024 character limit on SQL
statements and many of my SQL statements are more complex that that
permits. So I have to do some queries in steps creating temporary tables
rather than one big query. I am not sure if investing the time to learn
[dbplyr](https://dbplyr.tidyverse.org/) might be a neat tidyverse
solution to this, but I have been using SQL for almost 40 years and
perhaps I getting long in the tooth to learn quite so many new tricks.

**NB. DuckDB can take a long time to install, a longer time than you
think is reasonable but I have never had it crash on installing so be
patient**

## Caveat

This is work in progress and is an amalgamation of functions I have
written over the years and found useful but exist in many disparate
packages. This is my attempt to put the really useful ones in one place.
There are still some to import, namely the HES APC data ONS data and IMD
data import. There are also other functions to be written yet.

There are a suite of matching GOLD data processing functions that as yet
have not been imported

## Install

It is available at GitHub <https://github.com/PietaSchofield/rtrhd>

<div style="text-align: right">

**R code**

</div>

``` r
if(!require("remotes")){
  install.packages("remotes")
}
#> Loading required package: remotes
if(!require("tidyverse")){
  install.packages("tidyverse")
}
#> Loading required package: tidyverse
#> ── Attaching core tidyverse packages ──────────────────────────────────────────────────────── tidyverse 2.0.0 ──
#> ✔ dplyr     1.1.4     ✔ readr     2.1.5
#> ✔ forcats   1.0.0     ✔ stringr   1.5.1
#> ✔ ggplot2   3.5.1     ✔ tibble    3.2.1
#> ✔ lubridate 1.9.3     ✔ tidyr     1.3.1
#> ✔ purrr     1.0.2     
#> ── Conflicts ────────────────────────────────────────────────────────────────────────── tidyverse_conflicts() ──
#> ✖ dplyr::filter() masks stats::filter()
#> ✖ dplyr::lag()    masks stats::lag()
#> ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
if(!require("ggplot2")){
  install.packages("ggplot2")
}
if(!require("rtrhd")){
  remotes::install_github("PietaSchofield/rtrhd")
}
#> Loading required package: rtrhd
```

## Setup

I have access to the CPRD Aurum Synthetic Dataset which I will I can
share with folk at Liverpool University as we have Multi-Study License.
But I cannot distribute with this package. It is what I use in this
demo. Unfortunately it only has single files for each table unlike the
usual many many observation and drug_issue files. So it doesn’t totally
test the functionality of the table load functions.

### Paths

It is important the patient data and the lookups are in separate
directory trees and while the code recursively searches down the patient
data tree for files so they can be in subdirectories by table name (or
not) the code identifies files to import by wildcards and having the
Patienttype lookup in the same subtree as the Patient file(s) will break
the import of patients.

### Make the DuckDb

I will use the duckdb package because it is reasonalbly fast for some
analytics processing. It is slightly better tuned to this task than the
current itteration of SQLite and I am not going to write this to be
flexible enough to use a generic DBI SQL connection. I will leave that
as a task for the dedicated and just make the source code fully
available

<div style="text-align: right">

**R code**

</div>

``` r
dbName <- "Aurum_Sythetic_Data.duckdb"
dbPath <- file.path(Sys.getenv("HOME"),"Projects","rtrhd")
patient_data <- file.path(dbPath,"Aurum_Synthetic_Data","patient_data")
lookup_data <- file.path(dbPath,"Aurum_Synthetic_Data","lookups")
  
synthdataDb <- rtrhd::make_aurum_database(dbName,dbPath,
                 patient_path=file.path(data_path,"patient_data"),
                 lookup_path=file.path(data_path,"lookups"))
#> 4491905 records exist
#> 4535639 records exist
#> 124552 records exist
#> 14 records exist
#> 87679 records exist
#> 111313 records exist
#> 12257 records exist
```

This function wraps a bunch of functions that load the individual tables

<div style="text-align: right">

**R code**

</div>

``` r
tables <- rtrhd::list_tables(synthdataDb) 
names(tables) <- tables 
records <- lapply(tables, function(tn){
    flds <- paste0(rtrhd::list_fields(dbf=synthdataDb,tab=tn),collapse=", ")
    recs <- rtrhd::get_table(dbf=synthdataDb,
              sqlstr=paste0("SELECT COUNT(*) AS records FROM ",tn,";")) %>% pull(records)
    return(tibble(table=tn,fields=flds,records=recs))}) %>% bind_rows()
records %>% kableExtra::kable()
```

| table                   | fields                                                                                                                                                 | records |
|:------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------|--------:|
| aurum_common_dosages    | dosageid, dosage_text, daily_dose, dose_number, dose_unit, dose_frequency, dose_interval, choice_of_dose, dose_max_average, change_dose, dose_duration |   73068 |
| aurum_conssource        | conssourceid, description                                                                                                                              |   88013 |
| aurum_emiscodecat       | emiscodecatid, description                                                                                                                             |      48 |
| aurum_gender            | genderid, description                                                                                                                                  |       4 |
| aurum_jobcat            | jobcatid, description                                                                                                                                  |     208 |
| aurum_medicaldictionary | medcodeid, term, originalreadcode, cleansedreadcode, snomedctconceptid, snomedctdescriptionid, release, emiscodecategoryid                             |  240486 |
| aurum_numunit           | numunitid, description                                                                                                                                 |   18436 |
| aurum_obstype           | obstypeid, description                                                                                                                                 |      11 |
| aurum_orgtype           | orgtypeid, description                                                                                                                                 |     328 |
| aurum_parentprobrel     | parentprobrelid, description                                                                                                                           |       3 |
| aurum_patienttype       | patienttypeid, description                                                                                                                             |      32 |
| aurum_probstatus        | probstatusid, description                                                                                                                              |       4 |
| aurum_productdictionary | prodcodeid, dmdid, term from emis, productname, formulation, routeofadministration, drugsubstancename, substancestrength, bnfchapter, release          |  112148 |
| aurum_quantunit         | quantunitid, description                                                                                                                               |      97 |
| aurum_refmode           | refmodeid, description                                                                                                                                 |       7 |
| aurum_refservicetype    | refservicetypeid, description                                                                                                                          |      14 |
| aurum_refurgency        | refurgencyid, description                                                                                                                              |       5 |
| aurum_region            | regionid, description                                                                                                                                  |      13 |
| aurum_sign              | signid, description                                                                                                                                    |       2 |
| consultations           | patid, consid, consdate, conssourceid, cprdconstype, consmedcodeid                                                                                     | 4535639 |
| drug_issues             | patid, prodcodeid, issuedate, dosageid, quantity, quantunitid, duration                                                                                |   87679 |
| observations            | patid, consid, parentobsid, probobsid, medcodeid, obsdate, value, numunitid, numrangelow, numrangehigh                                                 | 4491905 |
| patients                | patid, pracid, gender, yob, regstartdate, regenddate, emis_ddate, cprd_ddate                                                                           |   45662 |
| practices               | pracid, lcd, uts, region                                                                                                                               |      14 |
| problems                | patid, obsid, parentprobobsid, probenddate, expduration, lastrevdate, parentprobrelid, probstatusid, probeddate                                        |  111313 |
| referrals               | patid, obsid, pracid, refsourceorgid, reftargetorgid, refurgencyid, refservicetypeid, refmodeid                                                        |  124552 |
| staff                   | staffid, pracid, jobcatid                                                                                                                              |   12257 |

It is now possible to run SQL against this database

<div style="text-align: right">

**R code**

</div>

``` r
## START SQL GEN BLOCK
unnamed_sql <- str_c("SELECT DISTINCT
  p.patid,
  COUNT(o.medcodeid) AS observations
FROM
  patients AS p
INNER JOIN
  observations AS o
  ON p.patid=o.patid
GROUP BY
  p.patid")
tmp_sqlfile <- rtrhd::link_sql(unnamed_sql)
```

<div style="text-align: right">

**SQL code**

</div>

``` sql
SELECT DISTINCT
  p.patid,
  COUNT(o.medcodeid) AS observations
FROM
  patients AS p
INNER JOIN
  observations AS o
  ON p.patid=o.patid
GROUP BY
  p.patid
```

<div style="text-align: right">

**R code**

</div>

``` r
rtrhd::unlink_sql(tmp_sqlfile)
res <- rtrhd::get_table(dbf=synthdataDb,sqlstr=unnamed_sql)
res %>% ggplot(aes(x=observations)) + geom_histogram(binwidth=10,fill="blue",colour="black",alpha=0.7) +
  labs(title="Distribution of Observation Counts Per Patient",
       x="Number of Observations",
       y="Frequency") + theme_minimal()
```

![](/home/pietas/GitLab/rtrhd/README_files/figure-gfm/unnamed-chunk-6-1.png)<!-- -->

``` r
## END SQL BLOCK
```

## Other Stuff

There are several functions for importing SNOMED CT and DMplusD data
available from TRUD. However the current manefestation of the DMplusD
import uses R to import the xml files and this is very very slow unless
your computer is powerful and a has a lot of memory. It still takes
significant time on my 20 core 64GB RAM machine. I have found a quicker
way to do this using calls to python scripts but this is not fully
implemented yet.

Talking of slow. Duckdb takes a long time to install first time round
and when it update it can also be slow but I think it is worth it.

## Finally

This has been developed on linux and not install and tested on windows
or mac so there is not guarantee it will work. It is actively being
developed and while I will probably try to not change functionality it
is definitely a moving target and as such I haven’t yet started to
version control it. It is currently morphing on a bleeding edge model.
If anyone else does actually start using it I will make the effort to
change to a release model. But as I am the only user at the moment it
hasn’t seemed necessary.

I also haven’t done clean install to check that I have all the
dependencies in the Imports list so there maybe some I haven’t caught
yet.

# By The Way

This is the code that built the vignette and the README markdown for the
GitHub repository

<div style="text-align: right">

**R code**

</div>

``` r
homeDir <- Sys.getenv("HOME")
packagepath <- file.path(homeDir,"GitLab","rtrhd")
inputpath <- file.path(packagepath,"vignettes")
inputFile <- file.path(inputpath,"rtrhd_demo.Rmd")
noteDir <- file.path("/srv","http","uol","rtrhd")

fdisp <- rmarkdown::render(inputFile,encoding=encoding,output_dir=noteDir,clean=T)


rmarkdown::render(inputFile, output_format = "github_document", output_file = "README.md",
                  output_dir = packagepath)
browseURL(fdisp)
```
