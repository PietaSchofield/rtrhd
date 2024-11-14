# R Tools For Routine Health Data rtrhd

This is really a bit of a miss-nomer it should probably be something like One Package to Rule them all.
It is a collection of convenience functions for taking data from CPRD (Aurum and GOLD) or from SNOMED or
NHSBSA DMplusD and ETLing it into duckdb databases so I can wrangle it with SQL to extract smaller
subsets into R for processing.

The functions are comming from 2 other packages cprdaurumtools and cprdgoldtools and I am merging these
2 with the addition of some tools to handle SNOMED CT and DMplusD. I hope to include READ and ICD codes
as well

## Install 

```
  devtools::install_github("PietaSchofield/rtrhd", build_vignettes = TRUE)
```

## Disclaimer

This software is licensed under the MIT License, which allows for free use, modification, and
distribution. However, it is provided "as is", without warranty of any kind, express or implied,
including but not limited to the warranties of merchantability, fitness for a particular purpose, and
noninfringement.

By using this software, you acknowledge that:

- The authors are not liable for any damages or issues arising from its use.
- The responsibility for verifying the software's suitability for your specific use case lies with you.

For more details, see the LICENSE file.


 


