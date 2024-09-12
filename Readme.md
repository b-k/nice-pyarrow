# Nicer Pyarrow tables

The ASF's Pyarrow vectors are extremely fast, as long as you use the specialized functions
in the `pyarrow.compute` submodule, where you might write
```
pc.add(pc.multiply(a, b), c)
```

It would clearly be preferable to be able to write `a * b + c`.
The solution: this package provides a wrapper class named `NiceVec` which provides
the binary infix we're all used to.

Usage: this is a one-file package. Copy `NiceTab.py` to your project directory and add
```
from NiceTab import NiceTab, NiceVec
```
To keep it even simpler, you may not even need to explicitly include `NiceVec`,
as a typical usage is to read data into a `NiceTab` table, where column operations on the table
return `NiceVec`s without any further effort on your part.

Pyarrow tables are simply lists of Pyarrow vectors and their names.
Along similar lines, this package provides the `NiceTab`, whose constituents are of type `NiceVec`.
The getter and setter methods of the `NiceTab` object are 
written so you can refer to columns directly. For example, if `tab` is a `NiceTab` with columns
`c1` and `c2`, one could add a few columns  to the table via, e.g.:

```
tab.newcol = tab.c1 * tab.c2 + 12
tab["zerocol"] = 0
nicevec_not_in_a_tab = tab.c1 / tab.c2
```

If the column you are setting already exists, it is overwritten.

- All operations are component-wise. The expected typical use is when each row of the table is a
separate observation unrelated to the others.
(See below for the `Q` function to gather aggregate statistics.)
For example, in `world_demo.py`, we take in data with one country per row and calculate national territorial
CO2 per capita via
```
data.co2_per_cap = data.entcari / data.npopuli * 1000000
```
The source data is in millions of tons of CO2, but we can multiply by a scalar to convert to tons,
which is more appropriate for a per capita statistic.

- To get specific rows, use `tab.filter(condition)`, where `condition` is either a vector (see logical
  operations on `NiceVec`s below), or a lambda that takes this `NiceTab` as input and returns a
  vector (see lambda mini-tutorial below).

- To build a table with only specific columns, use `tab.select(["c1", "c2", "c3"])`, with a list of desired column names.

- Where sensible, functions return `self`, which makes it possible to write chains like
```
subset = tab.filter(tab.x > 0).select(["y", "z"])
```

- Sometimes you just need a column of zeros. Use `tab.zeros()` to get a column of the appropriate
  size. If you need a column of ones, try `ones = tab.zeros() + 1`.

- The `NiceTab` has a function named `Σ` which allows summing multiple columns
within one table, like `income = tab.Σ("sales_income", "interest_income", "rental_income")`.

- If you want to act directly on the underlying data table (formally, `pa.Table`) held by the `NiceTab` struct, use
its `.tab` element.


## `NiceVec`
As above, the primary goal of the `NiceVec` is to facilitate writing legible files with extensive calculations.
But it has some other conveniences:

- If `v` is a `NiceVec`, `len(v)` and `[f(x) for x in v]` do what you expect. `v[8]` returns the
  eigth element, as a typical python int, float, or string (not a Pyarrow scalar).

- You can't do `sum(v)`, because of clashes between Pyarrow scalars and Python scalars, but
  `v.sum()` works, and returns a Python scalar.
  If you need a weighted sum with weights `w`, try `(w*v).sum()`, or if the weights don't sum to
  one, `(w*v).sum()/w.sum()` (or use the querying function below).

- The `NiceVec` also has a `clip` method, where `nv.clip(lower=x, upper=y)` returns a result
equivalent to elementwise `max(x, min(nv, y))`. The most common usage, with no arguments like
`nv.clip()`, returns a vector where all negative values have been replaced with zeros.

- The `NiceVec` also supports `<, <=, >, >=, ==, !=, &, |`, where `&` is read as
"and" and `|` as "or". Then, given a `NiceTab` named `s`, `s.net_income > s.total_deductions`
returns a (`NiceVec`) vector of true/false values depending on the status of each observation.

- You can selectively replace elements using `replace_if`.
If we wanted to reimplement the clip function, we could perhaps write
```
clipped_net_in = (s.net_in - s.total_deductions).replace_if(s.net_in < s.total_deductions, 0).`
```

- If you want to act directly on the underlying vector (formally, `pa.ChunkedArray`) held by the `NiceVec` struct, use
its `.vec` element.


# Querying and output

The `Q` method of the `NiceTab` is intended to emulate the sort of queries one might do with basic SQL.
By default it returns a Pandas data frame, under the presumption that statistics about a large data set are
small tables whose values will be used outside the Pyarrow context.
Use the `select` and `filter` method above if you want to keep your data in Pyarrow tables.

A query with all the options for a `NiceData` table `d` might look like
`d.Q(["net_income", "total_deductions"], year=2023, aggregation="sum",
        where=(net_income > 0) & (total_deductions == deduction_limit), group_by="firm_size"])`

- The first argument is the single column or a list of columns to select. If you omit it, you get every column.
- The `where` clause restricts to subsets, filtering only those columns for which the condition is true.
  See the discussion below for the sort of true/false conditions you can put here, and the `lambdas`
    mini-tutorial below on advanced usage.
- The `aggregation` tells us what summary statistic you want, if any. Options include `"sum"`,
      `"max"`, `"min"`, `"mean"` or `"avg"`, `"count"`.
- The `group_by` tells us whether the aggregation is over subsets. If absent, use the entire table.
  As of this writing, takes only one column name.
- Specify the `weights` column if you need a weighted sum or other aggregation. If you
  want a count and weights are set, I ignore your `select` clause and assume you want a total weight
  in the groups or the full data set.
- If `weights_normalize==True`  (the default) and `weights` is provided, and you are aggregating,
  scale by `1./sum(weights)` after multiplying by the weights column. Set to `False` for sums with
  replicate weights or if you are very confident that `sum(weights)==1`.

Output given aggregation is a Pandas data frame whose index is the `group_by` column if there is one,
and whose columns are your `select` columns.
If you queried one statistic from one column, your statistic is in position (0, 0), which you might
get via, e.g., `tab.Q("col", aggregation='avg').iloc[0,0]`.
If you grouped aggregates regarding columns `c1` and `c2` by a column with categories A, B, and C,
you will interrogate the output with something like
```
result = tab.Q(['c1', 'c2'], aggregation='sum', group_by='alphabet')
print(result['c1']['A']
print(result['c2']['B']
```

# Demo
The file `world_demo.py` is a short script executing a few common tasks: reading in data, filtering
it, generating a column based on a calculation, querying it for statistics, writing output.

The data set, originally from [the World in Data](https://wid.world/data/) is 187kb, which felt disproportionate for distribution with an 11kb library.
To run the demo, get the data from your command line via
```
wget https://github.com/b-k/large-files/raw/master/world_data.psv
```
, or use your browser to get that URL and save it to this directory.

Given the data, run the demo via `python3 world_demo.py`.
For those of you who do run it: CN=China, QA=Qatar.


## Tests
There are a few simple tests in `tests.py`. Run them via
```
python3 -m unittest tests.py
```
Although not documented, `tests.py` may also be useful to you as a source of sample usages.


## Appendix: Lambdas

A very brief Python lesson: simple functions can be given a name like any old
float or int, and sent as arguments to other functions.

When used directly, it's easy to use the `NiceTab.Q` method to
print all positive incomes less than $10k by adding a "where" clause to the query:

```
print(data.Q("net_income", where=(data.net_income < 10000) & (data.net_income > 0)))
```
Here, before `data.Q` is called, the conditions evaluate to a column of true/false values;
the body of the function never sees what your conditions were, just the post-evaluation vector.

But say that you wrote a function that takes in a data set, does some pre-processing, then
does a query.  In this case, we can't pre-calculate the column of true/false values,
and need to send the _formula_ for evaluating the condition to the query, not the final evaluation.
Referencing abstract type theory, Python lets you define a function and assign
it to a variable using the `lambda` keyword:

```
C=lambda tab: (tab.net_income < 10000) & (tab.net_income > 0)

#Is largely equivalent to

def C(tab: NiceTab) -> NiceVec: return (tab.net_income < 10000) & (tab.net_income > 0)
```

Now that we have defined a pocket function, we can send it to our query
functions, such as `augmented_Q(indata, "net_income", where=C)`. The `C`
function is called to generate true/false values by the query function as it runs.

The `where` argument to `Q` and the `filter` method both accept lambdas of the above type (`NiceTab -> NiceVec`).

## License

This was written by a U.S. Government employee while on duty, so Title 17 of the U.S. Code
(copyright) does not apply.
As a courtesy, please cite if used for anything with a bibliography:

```
@misc{nicetabs,
    author = {Ben Klemens},
    title = {NiceTabs tabular wrappers},
    year = {2024},
    url = {https://github.com/b-k/nicetabs}
}
```
