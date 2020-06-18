#__precompile__()

module LAJuliaUtils

export addCols!, pivot, customSort!, toDict, findall, unzip


#todo: customSort!, toDict, defEmptyIT, defVars, fillMissings!, toDataFrame

using DataFrames, DataStructures, IndexedTables, ZipFile #, Missings#, DataFramesMeta  # DataFramesMeta , SymPy,  QuadGK
import Base.findall


##############################################################################
##
## addCols!()
##
##############################################################################

"""
    addCols!(df, colsName, colsType)

Adds to the dataframe empty column(s) colsName of type(s) colsType

# Arguments
* `df`: the dataframe to add the columns to
* `colsName=[]`: the name(s) of columns to add (symbols)
* `colsType=[]`: the type(s) of columns to add. Default to Any

# Notes
* If colsName is a vector and colsType is a single value, all columns will be created with this type

# Examples
```julia
julia> using DataFrames, LAJuliaUtils
julia> addCols!(df,[:col1,:col2],Int)
```
"""
function addCols!(df::DataFrame, colsName::Union{Symbol, Vector{Symbol}}, colsType::Union{DataType, Vector{DataType}} = DataType[])

    colsNameV = []
    colsTypeV = []
    sfSize = size(df, 1)
    if(isa(colsName, Array))
        colsNameV = colsName
    else
        push!(colsNameV,colsName)
    end
    if(isa(colsType, Array))
        colsTypeV = colsType
    else
        push!(colsTypeV,colsType)
    end
    if length(colsTypeV) == 0
      for i in colsNameV
          push!(colsTypeV,Any)
      end
    end
    if length(colsTypeV) == 1
      for i in range(1,length(colsNameV)-1)
          push!(colsTypeV,colsTypeV[1])
      end
    end

    if length(colsNameV) != length(colsTypeV)
        error("colsName must have the same length of colsType")
    end
    for (i,e) in enumerate(colsNameV)
        df.e = Array{Union{colsTypeV[i],Missing},1}(missing,sfSize)
    end

    return df

end


##############################################################################
##
## pivot()
##
##############################################################################
"""
Pivot and optionally filter and sort in a single function

```julia
pivot(df::AbstractDataFrame, rowFields, colField, valuesField; <keyword arguments>)
```

# Arguments
* `df::AbstractDataFrame`: the original dataframe, in stacked version (dim1,dim2,dim3... value)
* `rowFields`:             the field(s) to be used as row categories (also known as IDs or keys)
* `colField::Symbol`:      the field containing the values to be used as column headers
* `valuesField::Symbol`:   the column containing the values to reshape
* `ops=sum`:               the operation(s) to perform on the data, default on summing them (see notes)
* `filter::Dict`:          an optional filter, in the form of a dictionary of column_to_filter => [list of ammissible values]
* `sort`:                  optional row field(s) to sort (see notes)

# Notes
* ops can be any supported Julia operation over a single array, for example: `sum`, `mean`, `length`, `countnz`, `maximum`, `minimum`, `var`, `std`, `prod`.
  Multiple operations can be specified using an array, and in such case an additional column is created to index them.
* filters are optional. Only `in` filter is supported.
* sort is possible only for row fields. Using a touple instead of just `:colname` you can specify reverse ordering (e.g. `(:colname, true)`) or a custom sort order (e.g. `(:colname, [val1,val2,val3])`. Elements you do not specify are not sorted but put behind those that you specify).
  You can pass multiple columns to be sorted in an array, e.g. [(:col1,true),:col2,(:col3,[val1,val2,val3])].

# Examples
```julia
julia> using DataFrames, LAJuliaUtils
julia> df = DataFrame(region   = ["US","US","US","US","EU","EU","EU","EU","US","US","US","US","EU","EU","EU","EU"],
                      product  = ["apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana"],
                      year     = [2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011],
                      produced = [3.3,3.2,2.3,2.1,2.7,2.8,1.5,1.3,  4.3,4.2,3.3,2.3,3.7,3.8,2.0,3.3],
                      consumed = [4.3,7.4,2.5,9.8,3.2,4.3,6.5,3.0,  5.3,7.4,3.5,9.8,4.2,6.3,8.5,4.0],
                      category = ['A','A','A','A','A','A','A','A', 'B','B','B','B','B','B','B','B',])
julia> longDf = DataFrames.stack(df,[:produced,:consumed])
julia> pivDf  = pivot(longDf, [:product, :region, :variable], :year, :value,
               ops    = [sum, mean, var],
               filter = Dict(:variable => ["produced"],:product => ["apple"]),
               sort   = [:product, (:region, true)]
              )
6×6 DataFrame
│ Row │ product │ region │ variable │ op     │ 2010     │ 2011     │
│     │ String  │ String │ Cat…     │ String │ Float64? │ Float64? │
├─────┼─────────┼────────┼──────────┼────────┼──────────┼──────────┤
│ 1   │ apple   │ US     │ produced │ mean   │ 3.8      │ 3.7      │
│ 2   │ apple   │ US     │ produced │ sum    │ 7.6      │ 7.4      │
│ 3   │ apple   │ US     │ produced │ var    │ 0.5      │ 0.5      │
│ 4   │ apple   │ EU     │ produced │ mean   │ 3.2      │ 3.3      │
│ 5   │ apple   │ EU     │ produced │ sum    │ 6.4      │ 6.6      │
│ 6   │ apple   │ EU     │ produced │ var    │ 0.5      │ 0.5      │
```
"""
function pivot(df::AbstractDataFrame, rowFields, colField::Symbol, valuesField::Symbol; ops=sum, filter::Dict=Dict(), sort=[])

    #longDf, [:product, :region,], :year, :value,
    #df = longDf
    #rowFields = [:product, :region,]
    #colField = :year
    #valuesField = :value
    #ops=sum
    #filter=Dict()
    #sort=[]


    for (k,v) in filter
      df = df[ [i in v for i in df[!,k]], :]
    end

    sortv = []
    sortOptions = []
    if(isa(sort, Array))
        sortv = sort
    else
        push!(sortv,sort)
    end
    for i in sortv
        if(isa(i, Tuple))
            if (isa(i[2], Array)) # The second option is a custom order
                orderArray = Array(collect(union(    OrderedSet(i[2]),  OrderedSet(unique(df[i[1]]))        )))
                push!(sortOptions, order(i[1], by = x->Dict(x => i for (i,x) in enumerate(orderArray))[x] ))
            else                  # The second option is a reverse direction flag
                push!(sortOptions, order(i[1], rev = i[2]))
            end
        else
          push!(sortOptions, order(i))
        end
    end

    catFields::AbstractVector{Symbol} = cat(rowFields, colField, dims=1)

    dfs  = DataFrame[]
    opsv =[]
    if(isa(ops, Array))
        opsv = ops
    else
        push!(opsv,ops)
    end

    for op in opsv
        #dft = by(df, catFields) do df2
         dft = combine(DataFrames.groupby(df, catFields)) do df2
            a = DataFrame()
            a[!,valuesField] = [op(df2[!,valuesField])]
            if(length(opsv)>1)
                a[!, :op] .= string(op)
            end
            a
        end
        push!(dfs,dft)
    end

    df = vcat(dfs...)
    df = DataFrames.unstack(df,colField,valuesField)
    sort!(df, sortOptions)
    return df
end


##############################################################################
##
## customSort!()
##
##############################################################################

"""
    customSort!(df, sortops)

Sort a dataframe by multiple cols, each specifying sort direction and custom sort order.

# Arguments
* `df`: the dataframe to sort
* `sortops=[]`: the name(s) of column(s) to sort (symbol, array of symbols, tuples or array of tuples)

# Notes
* Using a touple instead of just `:colname` you can specify reverse ordering (e.g. `(:colname, true)`) or a custom sort order (e.g. `(:colname, [val1,val2,val3])`).
* Elements you do not specify are not sorted but are put behind those that you specify.
* You can pass multiple columns to be sorted in an array, e.g. [(:col1,true),:col2,(:col3,[val1,val2,val3])].

# Examples
```julia
julia> using DataFrames, LAJuliaUtils
julia> df = DataFrame(
              c1 = ['a','b','c','a','b','c'],
              c2 = ["aa","aa","bb","bb","cc","cc"],
              c3 = [1,2,3,10,20,30],
            )
julia> customSort!(df, [(:c2,["bb","cc"]),(:c1,['b','a','c'])])
6×4 DataFrames.DataFrame
│ Row │ c1  │ c2   │ c3 │ c4 │
├─────┼─────┼──────┼────┼────┤
│ 1   │ 'a' │ "bb" │ 10 │ 1  │
│ 2   │ 'c' │ "bb" │ 3  │ 1  │
│ 3   │ 'b' │ "cc" │ 20 │ 1  │
│ 4   │ 'c' │ "cc" │ 30 │ 1  │
│ 5   │ 'b' │ "aa" │ 2  │ 1  │
│ 6   │ 'a' │ "aa" │ 1  │ 1  │
```
"""
function customSort!(df::DataFrame, sortops)
    sortv = []
    sortOptions = []
    if(isa(sortops, Array))
        sortv = sortops
    else
        push!(sortv,sortops)
    end
    for i in sortv
        if(isa(i, Tuple))
            if (isa(i[2], Array)) # The second option is a custom order
                orderArray = Array(collect(union(    OrderedSet(i[2]),  OrderedSet(unique(df[i[1]]))        )))
                push!(sortOptions, order(i[1], by = x->Dict(x => i for (i,x) in enumerate(orderArray))[x] ))
            else                  # The second option is a reverse direction flag
                push!(sortOptions, order(i[1], rev = i[2]))
            end
        else
          push!(sortOptions, order(i))
        end
    end
    return sort!(df, sortOptions)
end

##############################################################################
##
## toDict()
##
##############################################################################

"""
    toDict(df, dimCols, valueCol)

Convert a DataFrame in a dictionary, specifying the dimensions to be used as key and the one to be used as value.

# Arguments
* `df`: the dataframe to convert
* `dimCols`: the dimensions to be used as key (in the order given)
* `valueCol`: the dimension to be used to store the value

# Examples
```julia
julia> using DataFrames, LAJuliaUtils
julia> df = DataFrame(
                colour = ["green","blue","white","green","green"],
                shape = ["circle", "triangle", "square","square","circle"],
                border = ["dotted", "line", "line", "line", "dotted"],
                area = [1.1, 2.3, 3.1, 4.2, 5.2]
            )
julia> myDict = toDict(df,[:colour,:shape,:border],:area)
Dict{Any,Any} with 4 entries:
  ("green", "square", "line")   => 4.2
  ("white", "square", "line")   => 3.1
  ("green", "circle", "dotted") => 5.2
  ("blue", "triangle", "line")  => 2.3
```
"""
function toDict(df, dimCols, valueCol)
    toReturn = Dict()
    for r in eachrow(df)
        keyValues = []
        [push!(keyValues,r[d]) for d in dimCols]
        toReturn[(keyValues...,)] = r[valueCol]
    end
    return toReturn
end


##############################################################################
##
## findall()
##
##############################################################################

"""
    findall(pattern,string,caseSensitive=true)

Find all the occurrences of `pattern` in `string`.

# Arguments
* `pattern`:       A String or a Regex to lookup
* `string`:        The String where to lookup
* `caseSensitive`: Whenever look for a caseSensitive match (default: true)

`findall` uses internally `findnext()` and returns an array of UnitRange with the ranges of the patterns (including interdependent ones).

If pattern is not found an empty vector is returned.

# Examples
```julia
julia> st = "Today is a fresh day: not too warm, not to cold, just fresh!"
"Today is a fresh day: not too warm, not to cold, just fresh!"
julia> ranges = findall("fresh",st)
2-element Array{UnitRange{Int64},1}:
 12:16
 55:59
julia> st2 = "aaffssffssffbbffssffcc"
"aaffssffssffbbffssffcc"
julia> ranges = findall("ffssff",st2)
3-element Array{UnitRange{Int64},1}:
 3:8
 7:12
 15:20
julia> ranges = findall("zz",st2)
0-element Array{UnitRange{Int64},1}
```
"""
function findall(pattern,string::AbstractString,caseSensitive=true)
    toReturn = UnitRange{Int64}[]
    s = 1
    if (!caseSensitive)
      pattern = lowercase(pattern)
      string = lowercase(string)
    end
    while true
        range = findnext(pattern,string,s)
        if range == nothing
             break
        else
            push!(toReturn, range)
            s = first(range)+1
        end
    end
    return toReturn
end


# ##############################################################################
# ##
# ## unzip()
# ##
# ##############################################################################

"""
  unzip(file,exdir="")

Unzip a zipped archive using ZipFile

# Arguments
* `file`:    a zip archive to unzip and extract (absolure or relative path)
* `exdir=""`: an optional directory to specify the root of the folder where to extract the archive (absolute or relative).

# Notes:
* The function doesn't perform a check to see if all the zipped files have a common root.

# Examples
```julia
julia> unzip("myarchive.zip",exdir="mydata")
```
"""
function unzip(file,exdir="")
    fileFullPath = isabspath(file) ?  file : joinpath(pwd(),file)
    basePath = dirname(fileFullPath)
    outPath = (exdir == "" ? basePath : (isabspath(exdir) ? exdir : joinpath(pwd(),exdir)))
    isdir(outPath) ? "" : mkdir(outPath)
    zarchive = ZipFile.Reader(fileFullPath)
    for f in zarchive.files
        fullFilePath = joinpath(outPath,f.name)
        if (endswith(f.name,"/") || endswith(f.name,"\\"))
            mkdir(fullFilePath)
        else
            write(fullFilePath, read(f))
        end
    end
    close(zarchive)
end


# ############################################################################
# #
# # toDataFrame()
# #
# ############################################################################
#
#
# toDataFrame(cols::Tuple, prefix="x") =
#     DataFrame(;(Symbol("$prefix$c") => cols[c] for c in fieldnames(cols))...)
# toDataFrame(cols::NamedTuples.NamedTuple, prefix="") =
#     DataFrame(;(c => cols[c] for c in fieldnames(cols))...)
# toDataFrame(cols::Array, prefix="y") =
#     DataFrame(;Symbol("y1") => [c for c in cols])
#
# """
#     toDataFrame(t)
#
# Convert an IndexedTable of type NDSparse to a DataFrame, maintaining column types and (eventual) column names.
#
# """
# toDataFrame(t::IndexedTables.NDSparse) =
#     hcat(toDataFrame(columns(keys(t))), toDataFrame(columns(values(t)),"y"))

# ##############################################################################
# ##
# ## defEmptyIT()
# ##
# ##############################################################################
#
# """
#   defEmptyIT(dimNames, dimTypes; <keyword arguments>)
#
# Define empty IndexedTable(s) with the specific dimension(s) and type(s).
#
# # Arguments
# * `dimNames`: array of names of the dimensions to define (can be empty)
# * `dimTypes`: array of types of the dimensions (must be same length of dimNames if the latter is not null)
# * `valueNames = []` array of names of the value cols to define (can be empty)
# * `valueTypes=[Float64]` array of types of the value cols to define (must be same length of valueNames if the latter is not null)
# * `n=1`: number of copies of the specified tables to return
#
# # Examples
# ```julia
# julia> price,demand,supply = defEmptyVars(["region","item","qclass"],[String,String,Int64],valueNames=["val2000","val2010"],valueTypes=[Float64,Float64],n=3 )
# julia> waterContent = defEmptyVars(["region","item"],[String,String])
# julia> price["US","apple",1] = 3.2,3.4
# julia> waterContent["US","apple"] = 0.2
# ```
#
# # Notes
# Single index or single column can not be associated to a name.
# """
# function defEmptyIT(dimNames, dimTypes; valueNames=[],valueTypes=[Float64],n=1)
#     toReturn = []
#     dimSNames = [Symbol(d) for d in dimNames]
#     valueSNames = [Symbol(d) for d in valueNames]
#     for i in 1:n
#         # inside the loop as they are passed by reference!
#         dimValues = [Array{T,1}() for T in dimTypes]
#         valueValues = [Array{T,1}() for T in valueTypes]
#         t = Any
#         if (length(dimTypes) > 1)
#             d = length(dimSNames)  > 0 ? Columns(dimValues..., names=dimSNames) : Columns(dimValues...)
#             if (length(valueTypes) > 1)
#                 v = length(valueSNames) > 0 ? Columns(valueValues..., names=valueSNames) : Columns(valueValues...)
#                 t = IndexedTables.NDSparse(d,v)
#             else
#                 t = IndexedTables.NDSparse(d,valueValues[1])
#             end
#         else
#             if (length(valueTypes) > 1)
#                 v = length(valueSNames) > 0 ? Columns(valueValues..., names=valueSNames) : Columns(valueValues...)
#                 t = IndexedTables.NDSparse(dimValues[1],v)
#             else
#                 t = IndexedTables.NDSparse(dimValues[1],valueValues[1])
#             end
#         end
#         if(n==1)
#             return t
#         else
#             push!(toReturn,t)
#         end
#     end
#     return (toReturn...,)
# end
#
# ##############################################################################
# ##
# ## defVars()
# ##
# ##############################################################################
#
# """
#   defVars(vars, df, dimensions;<keyword arguments>)
#
# Create the required IndexedTables from a common DataFrame while specifing the dimensional columns.
#
# # Arguments
# * `vars`: the array of variables to lookup
# * `df`: the source of the dataframe, that must be in the format parName|d1|d2|...|value
# * `dimensions`: the name of the column containing the dimensions over which the variables are defined
# * `varNameCol (def: "varName")`: the name of the column in the df containing the variables names
# * `valueCol (def: "value")`: the name of the column in the df containing the values
#
# # Examples
# ```julia
# julia> (vol,mortCoef)  = defVars(["vol","mortCoef"], forData,["region","d1","year"], varNameCol="parName", valueCol="value")
# ```
# """
# function defVars(vars, df, dimensions; varNameCol="varName", valueCol="value")
#     toReturn = []
#     sDimensions = [Symbol(d) for d in dimensions]
#     for var in vars
#         filteredDf = df[df[Symbol(varNameCol)] .== var,:]
#         #filteredDf = @where(df, _I_(Symbol(varNameCol)) .== var)
#         dimValues =  [toArray(filteredDf[Symbol(dim)]) for dim in dimensions]
#         values = toArray(filteredDf[Symbol(valueCol)])
#         t = IndexedTables.NDSparse(dimValues..., names=sDimensions, values)
#         if length(vars) > 1
#             push!(toReturn,t)
#         else
#             return t
#         end
#     end
#     return (toReturn...,)
# end
#
# ##############################################################################
# ##
# ## fillMissings!()
# ##
# ##############################################################################
#
# function explode(base, elements)
#     toReturn = []
#     for b in base
#         for e in elements
#             newkey = vcat(b,[e])
#             push!(toReturn,newkey)
#         end
#     end
#     return toReturn
# end
#
# function fillkeys(dimensions)
#     toReturn = [[]]
#     for d in dimensions
#         toReturn = explode(toReturn,d)
#     end
#     return toReturn
# end
#
# """
#   fillMissings!(vars::IndexedTable, value, dimensions)
#
# For each values in the specified dimensions, fill the values of IndexedTable(s) without a corresponding key.
#
# As IndexedTables return a keyerror if they don't find the key, this funsction allows to "fill" the empty values
# with a given value.
#
# # Arguments
# * `vars`: the variable to fill the values. Can be either an Array of variables or a single one.
# * `value`: the value to be used to fill
# * `dimensions`: the arrays corresponding to the dimensions of the indexed table that one want to fill
#
# # Examples
# ```julia
# julia> fillMissings!(quantity, 0, [priProducts, fTypes, dClasses])
# ```
# """
# function fillMissings!(vars::AbstractArray{<:NDSparse,1}, value, dimensions)
#     allKeys = fillkeys(dimensions)
#     #varsv = isa(vars, Array)? vars:[vars]
#     for var in vars
#         varKeys = [values(k_idx) for k_idx in keys(var)]
#         for k in allKeys
#             if !(k in varKeys)
#                 var[k...] = value
#             end
#         end
#     end
#     return nothing
# end
# function fillMissings!(var::NDSparse, value, dimensions)
#     fillMissings!([var], value, dimensions)
#     return nothing
# end

# ##############################################################################
# ##
# ## plotBeta()
# ##
# ##############################################################################
#
# """
#     plotBeta(α,β)
#
# Plot the probability density function of the beta distribution (new plot).
#
# # Arguments
# * `α`
# * `β`
# """
# function plotBeta(α,β)
#     x = symbols("x")
#     a, b = symbols("a b", integers= true, positive=true)
#     Bfunction = quadgk(u->u^(α-1)*(1-u)^(β-1),0.0,1.0)[1]
#     Beta = 1/Bfunction * x^(a-1)*(1-x)^(b-1)
#     BetaResolved = subs(Beta,(a,α),(b,β))
#     plot(x,BetaResolved,0,1,show=true)
#     gui()
# end
#
# ##############################################################################
# ##
# ## plotBeta!()
# ##
# ##############################################################################
#
# """
#     plotBeta!(α,β)
#
# Plot the probability density function of the beta distribution (add to existing plot).
#
# # Arguments
# * `α`
# * `β`
# """
# function plotBeta!(α,β)
#     x = symbols("x")
#     a, b = symbols("a b", integers= true, positive=true)
#     Bfunction = quadgk(u->u^(α-1)*(1-u)^(β-1),0.0,1.0)[1]
#     Beta = 1/Bfunction * x^(a-1)*(1-x)^(b-1)
#     BetaResolved = subs(Beta,(a,α),(b,β))
#     plot!(x,BetaResolved,0,1,show=true)
#     gui()
# end

end # module LAJuliaUtils
