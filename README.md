# R Tools For Routine Health Data rtrhd

This is really a bit of a miss-nomer it should probably be something like One Package to Rule them all.
It is a collection of convenience functions for taking data from CPRD (Aurum and GOLD) or from SNOMED or
NHSBSA DMplusD and ETLing it into duckdb databases so I can wrangle it with SQL to extract smaller
subsets into R for processing.

The functions are comming from 2 other packages cprdaurumtools and cprdgoldtools and I am merging these
2 with the addition of some tools to handle SNOMED CT and DMplusD. I hope to include READ and ICD codes
as well

 


