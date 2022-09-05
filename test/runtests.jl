using Test
using DataFrames, LAJuliaUtils, Statistics


df = DataFrame(region   = ["US","US","US","US","EU","EU","EU","EU","US","US","US","US","EU","EU","EU","EU"],
               product  = ["apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana"],
               year     = [2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011],
               produced = [3.3,3.2,2.3,2.1,2.7,2.8,1.5,1.3,  4.3,4.2,3.3,2.3,3.7,3.8,2.0,3.3],
               consumed = [4.3,7.4,2.5,9.8,3.2,4.3,6.5,3.0,  5.3,7.4,3.5,9.8,4.2,6.3,8.5,4.0],
               category = ['A','A','A','A','A','A','A','A', 'B','B','B','B','B','B','B','B',])
longDf = DataFrames.stack(df,[:produced,:consumed])
pivDf  = pivot(longDf, [:product, :region, :variable], :year, :value,
               ops    = [mean, var],
               filter = Dict(:variable => ["produced"], :product => ["apple"]),
               sort   = [:product, (:region, true)]
              )
@test pivDf[!,Symbol(2010)] ≈ [ 3.8, 0.5,  3.2,  0.5]
