# lajuliautils

Utility functions for Julia, mainly dataframes operations.

Read more about them using the `?function` syntax

Currently provided functions:

* addCols!(df, colsName, colsType) - Adds to the DataFrame empty column(s) colsName of type(s) colsType
* pivot(df::AbstractDataFrame, rowFields, colField, valuesField; <keyword arguments>) - Pivot and optionally filter and sort in a single function
* customSort!(df, sortops) - Sort a DataFrame by multiple cols, each specifying sort direction and custom sort order
