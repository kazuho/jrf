# jrf - a JSON filter with the power and speed of Ruby

## SYNOPSIS

```sh
jrf 'STAGE >> STAGE >> STAGE ...' < input.ndjson
jrf --help

# Extract
jrf '_["foo"]'

# Filter then extract
jrf 'select(_["x"] > 10) >> _["foo"]'

# Aggregate
jrf 'select(_["item"] == "Apple") >> sum(_["count"])'
jrf 'percentile(_["ttlb"], 0.50)'
jrf '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'

# Flatten arrays into rows
jrf '_["items"] >> flat'

# Sort rows by key expression
jrf 'sort(_["at"]) >> _["id"]'

# Group by key and aggregate
jrf 'group_by(_["status"]) { count() }'
```

Build the man page from this README:

```sh
rake man
man -l man/jrf.1
```

## WHY RUBY?

No need to learn a new programming language! Just use Ruby to:
- write whatever filtering logic inside `select(...)`
- implement custom aggregation logic using `reduce(...) { block }`

In addition, `jrf` is extremely fast thanks to Ruby's JSON parser and the JIT.
In this workload/environment, a simple test shows over 3x boost compared to `jq`:

```sh
% time jq -s 'map(.tid) | min' < large.ldjson
327936
jq -s 'map(.tid) | min' < large.ldjson  4.90s user 0.46s system 99% cpu 5.395 total
% time jrf 'min(_["tid"])' < large.ldjson
327936
exe/jrf 'min(_["tid"])' < large.ldjson  1.37s user 0.15s system 99% cpu 1.531 total
```

## INPUT AND OUTPUT

- By default, input is NDJSON (one JSON value per line); empty lines are skipped.
- `--lax` allows multiline JSON texts and parses whitespace-delimited streams (also detects RS `0x1e` for JSON-SEQ).
- Output is NDJSON (one compact JSON value per line).

## BUILT-IN FUNCTIONS

`jrf` processes the input using a multi-stage pipeline that is connected by top-level `>>`.

Within each stage, the current JSON value is available as `_`, and the following built-in functions are provided.
For aggregation functions, `nil` values are ignored.

### select(predicate)

Filters rows. If predicate is true, the current value passes through; if false, the row is dropped.

```sh
jrf 'select(_["status"] == 200) >> _["path"]'
```

### flat

Expands an Array into multiple rows, one output row per element.

```sh
jrf '_["items"] >> flat'
```

### group
### group(expr)

Collects values into one Array. This is the opposite of `flat`.
`group` (without arguments) is shorthand for `group(_)`, i.e., collect the current stage value as-is.
`group(expr)` first evaluates `expr` and collects that result instead.

```sh
jrf '_["id"] >> group'
jrf 'group(_["id"])'
```

### average(expr)

Computes the average value across rows.

```sh
jrf '_["latency"] >> average(_)'
```

### min(expr)

Computes the minimum value across rows.

```sh
jrf '_["latency"] >> min(_)'
```

### max(expr)

Computes the maximum value across rows.

```sh
jrf '_["latency"] >> max(_)'
```

### stdev(expr)

Computes the standard deviation across rows.

```sh
jrf '_["latency"] >> stdev(_)'
```

### sum(expr)

Computes the sum across rows.

```sh
jrf '_["price"] * _["unit"] >> sum(_)'
```

### count()
### count(expr)

`count()` counts rows.
`count(expr)` counts non-`nil` values of `expr`.

```sh
jrf 'count()'
jrf 'select(_["status"] == 200) >> count()'
```

### percentile(expr, 0.95)
### percentile(expr, [0.1, 0.5, 0.9])

Computes percentiles for `p` in `[0.0, 1.0]`.

If a scalar is given as a percentile, emits the value as a scalar.

If an array of percentiles is given, emits one array of values in the same order as the requested percentiles.
For example, with `[0.1, 0.5, 0.9]`, the output is `[p10_value, p50_value, p90_value]`.

Example output:
```json
[38, 123, 469]
```

### reduce(initial) { |acc, v| ... }

Generic custom reducer API.
Most built-in aggregations are convenience wrappers around `reduce`, and many reshaping patterns can also be expressed with `reduce`.

```sh
jrf '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'
jrf '_["count"] >> reduce(0) { |acc, v| acc + v }'
```

### sort(key_expr)
### sort(key_expr) { |a, b| ... }

Sorts rows.
With one argument, rows are sorted by key expression.
With a block, rows are sorted by custom comparator.

```sh
jrf 'sort(_["at"]) >> _["id"]'
jrf 'sort { |a, b| b["at"] <=> a["at"] } >> _["id"]'
```

### map { |x| reducer(x) }

Applies a reducer to each element of an Array, element-wise across rows.
Each array position gets its own independent reducer instance.
Inside the block, `_` remains the surrounding row value; use the block parameter for the element.

```sh
jrf 'map { |x| sum(x) }'
# [1,10], [2,20], [3,30] → [6,60]

jrf '_["values"] >> map { |x| min(x) }'
```

### map_values { |v| reducer(v) }

Applies a reducer to each value of a Hash, key-wise across rows.
Each key gets its own independent reducer instance.
Inside the block, `_` remains the surrounding row value; use the block parameter for the value.

```sh
jrf 'map_values { |v| sum(v) }'
# {"a":1,"b":10}, {"a":2,"b":20} → {"a":3,"b":30}
```

### group_by(key_expr)
### group_by(key_expr) { reducer }

Groups rows by key expression and applies a reducer per group.

Without a block, collects rows into arrays (equivalent to `group_by(key) { group }`).

With a block, applies the given reducer independently per group.
Inside the block, `_` still refers to the surrounding row, and the current row is also yielded as the block parameter.

```sh
jrf 'group_by(_["status"])'
# → {"200":[...rows...],"404":[...rows...]}

jrf 'group_by(_["status"]) { count() }'
# → {"200":15,"404":3}

jrf 'group_by(_["status"]) { |row| average(row["latency"]) }'
# → {"200":42.5,"404":120.0}
```

## LICENSE

MIT
