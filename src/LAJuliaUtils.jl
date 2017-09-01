module LAJuliaUtils

export addCols!, pivot, customSort!, toDict, plotBeta, plotBeta!

using DataFrames, DataStructures, SymPy, Plots, QuadGK


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
        df[e] = DataArray(colsTypeV[i],sfSize)
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
julia> longDf = stack(df,[:produced,:consumed])
julia> pivDf  = pivot(longDf, [:product, :region,], :year, :value,
                      ops    = [mean, var],
                      filter = Dict(:variable => [:consumed]),
                      sort   = [:product, (:region, true)]
                     )
 8×5 DataFrames.DataFrame
 │ Row │ product  │ region │ op     │ 2010 │ 2011 │
 ├─────┼──────────┼────────┼────────┼──────┼──────┤
 │ 1   │ "apple"  │ "US"   │ "mean" │ 4.8  │ 7.4  │
 │ 2   │ "apple"  │ "US"   │ "var"  │ 0.5  │ 0.0  │
 │ 3   │ "apple"  │ "EU"   │ "mean" │ 3.7  │ 5.3  │
 │ 4   │ "apple"  │ "EU"   │ "var"  │ 0.5  │ 2.0  │
 │ 5   │ "banana" │ "US"   │ "mean" │ 3.0  │ 9.8  │
 │ 6   │ "banana" │ "US"   │ "var"  │ 0.5  │ 0.0  │
 │ 7   │ "banana" │ "EU"   │ "mean" │ 7.5  │ 3.5  │
 │ 8   │ "banana" │ "EU"   │ "var"  │ 2.0  │ 0.5  │
```
"""
function pivot(df::AbstractDataFrame, rowFields, colField::Symbol, valuesField::Symbol; ops=sum, filter::Dict=Dict(), sort=[])

    for (k,v) in filter
      df = df[ [i in v for i in df[k]], :]
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

    catFields::AbstractVector{Symbol} = cat(1,rowFields, colField)
    dfs  = DataFrame[]
    opsv =[]
    if(isa(ops, Array))
        opsv = ops
    else
        push!(opsv,ops)
    end

    for op in opsv
        dft = by(df, catFields) do df2
            a = DataFrame()
            a[valuesField] = op(df2[valuesField])
            if(length(opsv)>1)
                a[:op] = string(op)
            end
            a
        end
        push!(dfs,dft)
    end

    df = vcat(dfs)
    df = unstack(df,colField,valuesField)
    sort!(df, cols = sortOptions)
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
    return sort!(df, cols = sortOptions)
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
        toReturn[(keyValues...)] = r[valueCol]
    end
    return toReturn
end

##############################################################################
##
## plotBeta()
##
##############################################################################

"""
    plotBeta(α,β)

Plot the probability density function of the beta distribution (new plot).

# Arguments
* `α`
* `β`
"""
function plotBeta(α,β)
    x = symbols("x")
    a, b = symbols("a b", integers= true, positive=true)
    Bfunction = quadgk(u->u^(α-1)*(1-u)^(β-1),0.0,1.0)[1]
    Beta = 1/Bfunction * x^(a-1)*(1-x)^(b-1)
    BetaResolved = subs(Beta,(a,α),(b,β))
    plot(x,BetaResolved,0,1,show=true)
end

##############################################################################
##
## plotBeta!()
##
##############################################################################

"""
    plotBeta!(α,β)

Plot the probability density function of the beta distribution (add to existing plot).

# Arguments
* `α`
* `β`
"""
function plotBeta!(α,β)
    x = symbols("x")
    a, b = symbols("a b", integers= true, positive=true)
    Bfunction = quadgk(u->u^(α-1)*(1-u)^(β-1),0.0,1.0)[1]
    Beta = 1/Bfunction * x^(a-1)*(1-x)^(b-1)
    BetaResolved = subs(Beta,(a,α),(b,β))
    plot!(x,BetaResolved,0,1,show=true)
    gui()
end


end # module LAJuliaUtils
