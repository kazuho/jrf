# jr

## NAME

`jr` - JSON transformer with the power and speed of Ruby

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

## WHAT IS `jr`

`jr` is a JSON transformer with the power and speed of Ruby.

You can use your knowledge of Ruby to:
- write whatever filtering logic inside `select(...)`
- implement custom aggregation logic using `reduce(...) { block }`
- the current JSON value is available as `_`
- the stages are piped using an `>>` operator

Besides, `jr` is extremely fast thanks to Ruby's JSON parser and the JIT.
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

Pipelines are connected by top-level `>>`.

`select(predicate)`
Filters rows. If predicate is true, the current value passes through; if false, the row is dropped.

```sh
jr 'select(_["status"] == 200) >> _["path"]'
```

`flat`
Expands an Array into multiple rows, one output row per element.

```sh
jr '_["items"] >> flat'
```

`group(value = _)`
Collects values into one Array.

```sh
jr '_["id"] >> group'
```

`min(value)`
Computes the minimum value across rows.

```sh
jr '_["latency"] >> min(_)'
```

`max(value)`
Computes the maximum value across rows.

```sh
jr '_["latency"] >> max(_)'
```

`percentile(value, p_or_array)`
Computes percentiles for `p` in `[0.0, 1.0]`.
If `p_or_array` is an Array, emits multiple rows like `{"percentile": p, "value": result}`.

```sh
jr 'percentile(_["ttlb"], 0.50)'
jr 'percentile(_["ttlb"], [0.25, 0.50, 0.95])'
```

Percentile array output format:
```json
{"percentile": 0.5, "value": 123}
```

`reduce(initial) { |acc, v| ... }`
`reduce(value, initial: x) { |acc, v| ... }`
Generic custom reducer API.
Most built-in aggregations are convenience wrappers around `reduce`, and many reshaping patterns can also be expressed with `reduce`.

```sh
jr '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'
jr '_["count"] >> reduce(0) { |acc, v| acc + v }'
```

`sort(key = _, &compare)`
Sorts rows.
With one argument, rows are sorted by key expression.
With a block, rows are sorted by custom comparator.

```sh
jr 'sort(_["at"]) >> _["id"]'
jr 'sort { |a, b| b["at"] <=> a["at"] } >> _["id"]'
```

`sum(value, initial: 0)`
Computes the sum of values across rows.

```sh
jr '_["latency"] >> sum(_)'
```

## LICENSE

MIT
