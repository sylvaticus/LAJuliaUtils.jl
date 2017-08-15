# LAJuliaUtils

Utility functions for Julia, mainly dataframes operations.

Read more about them using the `?function` syntax (after you installed and imported the package).

This is NOT a Julia registered package:
* install it with `Pkg.clone("git@github.com:sylvaticus/LAJuliaUtils.jl.git")`
* import it with `using LAJuliaUtils`

Provided functions:

* addCols!(df, colsName, colsType) - Adds to the DataFrame empty column(s) colsName of type(s) colsType
* pivot(df::AbstractDataFrame, rowFields, colField, valuesField; <keyword arguments>) - Pivot and optionally filter and sort in a single function
* customSort!(df, sortops) - Sort a DataFrame by multiple cols, each specifying sort direction and custom sort order
* toDict(df, dimCols, valueCol) - Convert a DataFrame in a dictionary, specifying the dimensions to be used as key and the one to be used as value.
