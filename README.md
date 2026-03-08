# jrf - a JSON Filter with the Power and Speed of Ruby

## SYNOPSIS

```sh
jrf 'STAGE >> STAGE >> STAGE ...' < input.ndjson

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

- Input and output are NDJSON (one JSON value per line).
- Empty lines are skipped.
- Each non-empty line is parsed with `JSON.parse`.

## BUILT-IN FUNCTIONS

`jrf` processes the input using a multi-stage pipeline that is connected by top-level `>>`.

Within each stage, the current JSON value is available as `_`, and the following built-in functions are provided.

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

### percentile(expr, 0.95)
### percentile(expr, [0.1, 0.5, 0.9])

Computes percentiles for `p` in `[0.0, 1.0]`.

If a scalar is given as a percentile, emits the value as a scalar.

If an array of percentiles is given, the output format is:
```json
{"percentile": 0.1, "value": 38}
{"percentile": 0.5, "value": 123}
{"percentile": 0.9, "value": 469}
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

## LICENSE

MIT
