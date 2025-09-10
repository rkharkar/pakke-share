# Pakke PRT Project Data and Analysis

This Julia project provides the data and scripts required to reproduce the results in the accompanying publication.

## Project Structure
```
pakke-share/
├── src/
│   ├── Pakke.jl             # Main module file
│   ├── importers.jl         # Data import functionality
│   ├── utils.jl             # Utility functions
│   ├── analysis.jl          # Analysis code
│   └── visualization.jl     # Visualization code
├── data/                    # Data files
│   ├── Elephants PRT.boris  # Boris project file
│   └── milestones\_orig.csv # Dates of new training start
├── output/                  # Generated visualizations and tables
└── README.md                # This file
```

## Usage

Make sure you have the Julia programming language installed. To run the main module, first cd to the src folder in this project. Then, start your julia repl and run the following commands:

```julia
using Pkg
Pkg.activate("../")
Pkg.instantiate()
include("Pakke.jl")
```

This will create the output directory with all the relevant figures and spreadsheets. Spreadsheets will be in the output/data directory. The other sub-folders within the output folder contain event timelines for individual sessions.
