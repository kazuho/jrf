# jr - a JSON transformer with the power and speed of Ruby

## SYNOPSIS

```sh
jr 'STAGE >> STAGE >> STAGE ...' < input.ndjson

# Extract
jr '_["foo"]'

# Filter then extract
jr 'select(_["x"] > 10) >> _["foo"]'

# Aggregate
jr 'select(_["item"] == "Apple") >> sum(_["count"])'
jr 'percentile(_["ttlb"], 0.50)'
jr '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'

# Flatten arrays into rows
jr '_["items"] >> flat'

# Sort rows by key expression
jr 'sort(_["at"]) >> _["id"]'
```

## WHY RUBY?

No need to learn a new programming language! Just use Ruby to:
- write whatever filtering logic inside `select(...)`
- implement custom aggregation logic using `reduce(...) { block }`

In addition, `jr` is extremely fast thanks to Ruby's JSON parser and the JIT.
In this workload/environment, a simple test shows over 3x boost compared to `jq`:

```sh
% gunzip < large.json.gz | time jq -s 'map(.tid) | min'
327936
jq -s 'map(.tid) | min'  4.89s user 0.85s system 98% cpu 5.848 total
% gunzip < large.json.gz | time exe/jr 'min(_["tid"])'  
327936
exe/jr 'min(_["tid"])'  1.53s user 0.12s system 98% cpu 1.678 total
```

## INPUT AND OUTPUT

- Input and output are NDJSON (one JSON value per line):
- Empty lines are skipped.
- Each non-empty line is parsed with `JSON.parse`.

## BUILT-IN FUNCTIONS

`jr` procceses the input using a multi-stage pipeline that is connected by top-level `>>`.

Within each stage, the current JSON value is available as `_`, and following built-in functions are provided.

### select(predicate)

Filters rows. If predicate is true, the current value passes through; if false, the row is dropped.

```sh
jr 'select(_["status"] == 200) >> _["path"]'
```

### flat

Expands an Array into multiple rows, one output row per element.

```sh
jr '_["items"] >> flat'
```

### group(expr)

Collects values into one Array. This is the opposite of `flat`.

```sh
jr '_["id"] >> group'
```

### min(expr)
### max(expr)
### sum(expr)

Computes the minimum, maximum, and summation value across rows.

```sh
jr '_["latency"] >> min(_)'
```

`max(value)`
Computes the maximum value across rows.

```sh
jr '_["latency"] >> max(_)'
```

```sh
jr '_["price"] * _["unit"] >> sum(_)'
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
### reduce(value, initial: x) { |acc, v| ... }

Generic custom reducer API.
Most built-in aggregations are convenience wrappers around `reduce`, and many reshaping patterns can also be expressed with `reduce`.

```sh
jr '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'
jr '_["count"] >> reduce(0) { |acc, v| acc + v }'
```

### sort(key_expr)
### sort(key_expr) { |a, b| ... }

Sorts rows.
With one argument, rows are sorted by key expression.
With a block, rows are sorted by custom comparator.

```sh
jr 'sort(_["at"]) >> _["id"]'
jr 'sort { |a, b| b["at"] <=> a["at"] } >> _["id"]'
```

## LICENSE

MIT
