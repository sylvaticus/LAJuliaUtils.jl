module LAJuliaUtils

export addCols!

using DataFrames

"""
    addCols!(df, colsName, colsType)

Adds the columns colsName of type colsType to df

# Arguments
* `df`: the dataframe to add the columns to
* `colsName=[]`: the names of columns to add
* `colsType=[]`: the type of columns to add

# Notes
*

# Examples
```julia
julia>
```
"""
function addCols!(df::DataFrame, colsName::Union{Symbol, Vector{Symbol}} = Symbol[], colsType::Union{Symbol, Vector{Symbol}} = Symbol[])

end
