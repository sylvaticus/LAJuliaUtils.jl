# LAJuliaUtils

Utility functions for Julia, mainly dataframes operations.

Read more about them using the `?function` syntax (after the package has been installed and imported).

This is NOT a Julia registered package:
* install it with `Pkg.clone("https://github.com/sylvaticus/LAJuliaUtils.jl.git")`
* import it with `using LAJuliaUtils`

Provided functions:

* `addCols!(df, colsName, colsType)` - Adds to the DataFrame empty column(s) colsName of type(s) colsType
* `pivot(df::AbstractDataFrame, rowFields, colField, valuesField; <kwd args>)` - Pivot and optionally filter and sort in a single function
* `customSort!(df, sortops)`         - Sort a DataFrame by multiple cols, each specifying sort direction and custom sort order
* `toDict(df, dimCols, valueCol)`    - Convert a DataFrame in a dictionary, specifying the dimensions to be used as key and the one to be used as value.
* `toArray(DA;<keyword arguments>)`  - Convert a DataArray{T1} in a normal Array{T2,1}, specifying T2 and optionally removing missing elements.
* `toDataFrame(t)`                   - Convert an IndexedTable to a DataFrame, maintaining column types and (eventual) column names.
* `defEmptyIT(dimNames, dimTypes; <kwd args>)` - Define empty IndexedTable(s) with the specific dimension(s) and type(s).
* `defVars(vars, df, dimensions;<kwd args>)`   - Create the required IndexedTables from a common DataFrame while specifing the dimensional columns.
* `fillMissings!(vars, value, dimensions)` - For each values in the specified dimensions, fill the values of IndexedTable(s) without a corresponding key.
