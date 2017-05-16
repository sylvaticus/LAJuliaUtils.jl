# lajuliautils

Utility functions for Julia, mainly dataframes operations.

Read more about them using the `?function` syntax

Currently provided functions:

* addCols!(df, colsName, colsType) - Adds to the dataframe empty column(s) colsName of type(s) colsType
* pivot(df::AbstractDataFrame, rowFields, colField, valuesField; <keyword arguments>) - Pivot and optionally filter and sort in a single function


