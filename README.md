# jrf - a JSON filter with the power and speed of Ruby

## SYNOPSIS

```sh
# jrf reads files (or STDIN) and emits NDJSON or pretty-prints JSON values
jrf 'STAGE >> STAGE >> STAGE ...' < file.ndjson
jrf 'STAGE >> STAGE >> STAGE ...' file1.ndjson file2.ndjson.gz
jrf --lax 'STAGE >> STAGE >> STAGE ...' < multiline.json
jrf --lax 'STAGE >> STAGE >> STAGE ...' < file.jsonseq
jrf -o pretty '_' file.json file.ndjson
jrf -o tsv 'group_by(_["status"]) { |row| average(row["latency"]) }'
jrf --require ./my_helpers.rb 'my_method(_["value"])'
jrf --help

# Extract
jrf '_["foo"]'

# Filter then extract
jrf 'select(_["x"] > 10) >> _["foo"]'

# Aggregate
jrf 'select(_["item"] == "Apple") >> sum(_["count"])'
jrf 'percentile(_["ttlb"], 0.50)'
jrf '_["msg"] >> reduce(nil) { |acc, v| acc ? "#{acc} #{v}" : v }'

# Transform array elements
jrf 'map { |x| select(x >= 1) }'
jrf 'map { |x| x + 1 }'

# Transform object values
jrf 'map_values { |v| select(v >= 1) }'
jrf 'map_values { |v| v * 10 }'

# Flatten arrays into rows
jrf '_["items"] >> flat'

# Sort rows by key expression
jrf 'sort(_["at"]) >> _["id"]'

# Group rows into arrays by key
jrf 'group_by(_["status"])'

# Group by key and aggregate
jrf 'group_by(_["item"]) { |row| sum(row["count"] * row["price"]) }'

# Group by key and aggregate, using a global as a stash
jrf '$perc ||= 0.005.step(0.995, 0.01); group_by(_["group"]) { |row| percentile(row["score"], $perc) }'
```

**[Need help writing a filter? Ask ChatGPT!](https://chatgpt.com/g/g-69b209ff063481919ce61f7f7c204a21-jrf-helper)**

## WHY JRF?

I had been using `jq` for years, but its unique DSL was always a pain — I could never remember the syntax without looking it up. It is also slow on large inputs and eats up a lot of memory.

Then one day, a carefully-written `jq` script started swapping and ground to a halt. That was the last straw.

What I wanted was:
- SQL-like syntax for aggregation, e.g., `sum(cost * price)`
- extensibility backed by a popular programming language
- speed and memory efficiency

Ruby turned out to be a natural fit. Any Ruby expression can be used as an argument to the built-in functions — no special DSL to learn:

```sh
jrf 'select(_["path"] =~ /^\/api/)'
jrf 'sort(_["name"].downcase)'
```

When built-ins alone aren't enough, Ruby blocks let you extend the logic naturally; custom ruby code can be preloaded as well:

```sh
jrf 'group_by(_["status"]) { |row| average(row["latency"]) }'
jrf --require ./my_helpers.rb 'my_method(_)'
```

Ruby is also fast and memory-efficient: jrf’s core logic and user-supplied expressions are optimized together by the same [JIT](https://docs.ruby-lang.org/en/3.4/yjit/yjit_md.html), strings are copied only when necessary, and Ruby comes with a [heavily optimized JSON parser](https://byroot.github.io/ruby/json/2024/12/15/optimizing-ruby-json-part-1.html). As a result, `jrf` outperforms `jq` — here over 3x on a simple aggregation:

```sh
% time jq -s 'map(.tid) | min' < large.ldjson
327936
jq -s 'map(.tid) | min' < large.ldjson  4.90s user 0.46s system 99% cpu 5.395 total
% time jrf 'min(_["tid"])' < large.ldjson
327936
exe/jrf 'min(_["tid"])' < large.ldjson  1.37s user 0.15s system 99% cpu 1.531 total
```

Give it a try — install via RubyGems: `gem install jrf`

## INPUT AND OUTPUT

- By default, input is NDJSON (one JSON value per line); empty lines are skipped.
  - `--lax` allows multiline JSON texts and parses whitespace-delimited streams (also detects RS `0x1e` for JSON-SEQ).
  - If no filenames are provided, data is read from the standard input.
  - If the provided filename ends with `.gz`, the file is decompressed automatically.
- Output format is controlled by `-o`/`--output FORMAT`:
  - `json` (default) — one compact JSON value per line (NDJSON).
  - `pretty` — pretty-prints each output JSON value.
  - `tsv` — tab-separated values. Hashes become rows keyed by their keys; arrays of arrays become rows directly. Scalar and null cells are printed as-is; nested arrays and objects are rendered as compact JSON. Useful for pasting into spreadsheets or piping through `column -t`.
  - Short outputs are grouped into atomic writes (4 KB by default; configurable via `--atomic-write-bytes N`), allowing safe use with parallel pipelines such as `xargs -P`.

## BUILT-IN FUNCTIONS

`jrf` processes the input using a multi-stage pipeline that is connected by top-level `>>`.

Within each stage, the current JSON value is available as `_`, and the following built-in functions are provided.
Inside nested block contexts such as `map`, `map_values`, and `group_by`, `_` remains the surrounding row value, while implicit-input built-ins operate on the current target object for that block.
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
`group` (without arguments) collects the current target object as-is.
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

### count_if(condition)

Counts rows where `condition` is truthy.

```sh
jrf 'count_if(_["status"] == 200)'
jrf '[count_if(_["x"] > 0), count_if(_["x"] < 0)]'
```

### percentile(expr, 0.95)
### percentile(expr, [0.1, 0.5, 0.9])
### percentile(expr, 0.1.step(0.9, 0.4))

Computes percentiles for `p` in `[0.0, 1.0]`.

If a scalar is given as a percentile, emits the value as a scalar.

If an enumerable of percentiles is given, emits one array of values in the same order as the requested percentiles.
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
### sort

Sorts rows.
With one argument, rows are sorted by key expression.
With a block, rows are sorted by custom comparator.
Wit no argument, rows are sorted by the current target value. This is most useful when the target value is a number or a string.

```sh
jrf 'sort(_["at"]) >> _["id"]'
jrf 'sort { |a, b| b["at"] <=> a["at"] } >> _["id"]'
jrf 'sort'
```

### map { |x| ... }
### map(collection) { |x| ... }

Maps each element of an Array, or each entry of a Hash (yielding `[key, value]` pairs like Ruby's `Hash#map`), returning an Array.
By default operates on the current value; pass an explicit collection to operate on a different one.
Inside the block, `_` remains the surrounding row value; use the block parameter for the element.

If the block is a plain expression, `map` transforms each element per row.
If the block uses aggregations (e.g. `sum`), each array position (or hash key) gets its own independent accumulator across rows.

```sh
jrf 'map { |x| x + 1 }'

jrf 'map { |x| sum(x) }'
# [1,10], [2,20], [3,30] → [6,60]

jrf 'map { |(k, v)| "#{k}=#{v}" }'
# {"a":1,"b":10} → ["a=1","b=10"]

jrf 'map { |(k, v)| sum(v) }'
# {"a":1,"b":10}, {"a":2,"b":20} → [3,30]

jrf '_["values"] >> map { |x| min(x) }'

jrf 'map(_["items"]) { |x| x * 2 }'
```

### map_values { |v| ... }
### map_values(collection) { |v| ... }

Maps each value of a Hash and returns a Hash.
By default operates on the current value; pass an explicit collection to operate on a different one.
Inside the block, `_` remains the surrounding row value; use the block parameter for the value.

If the block is a plain expression, `map_values` transforms each value per row.
If the block uses aggregations, each key gets its own independent accumulator across rows.

```sh
jrf 'map_values { |v| v * 10 }'

jrf 'map_values { |v| sum(v) }'
# {"a":1,"b":10}, {"a":2,"b":20} → {"a":3,"b":30}
```

### apply { |x| ... }
### apply(collection) { |x| ... }

Runs an expression over the current value (an Array), processing all elements within that single value.
By default operates on the current value; pass an explicit collection to operate on a different one.
Unlike `map` which accumulates across rows (the same position across multiple inputs), `apply` aggregates within one value (all elements of a single array), completing immediately.
Inside the block, `_` remains the surrounding row value; use the block parameter for each element.

```sh
# normalize values by their sum
jrf 'total = apply { |x| sum(x) }; map { |x| x.to_f / total }'
# [3,7] → [0.3,0.7]

# aggregate a nested array
jrf 'map { |o| [o["name"], apply(o["scores"]) { |x| average(x) }] }'
```

### group_by(key_expr)
### group_by(key_expr) { ... }

Groups rows by key expression and applies a reducer per group.

Without a block, collects rows into arrays (equivalent to `group_by(key) { group }`).

With a block, applies the given reducer independently per group.
Inside the block, `_` still refers to the surrounding row, and the current row is also yielded as the block parameter.

```sh
jrf 'group_by(_["status"])'
# → {"200":[...rows...],"404":[...rows...]}

jrf 'group_by(_["item"]) { |row| sum(row["count"] * row["price"]) }'
# → {"Apple":1250,"Orange":830}

jrf 'group_by(_["status"]) { |row| average(row["latency"]) }'
# → {"200":42.5,"404":120.0}
```

### LIMITATIONS

Aggregation built-ins accept ordinary Ruby expressions as arguments, but their results are not ordinary Ruby values during evaluation. They can appear as standalone values in reducer templates such as a stage result, an array, a hash, or a reducer-aware block, but they cannot be combined with operators or wrapped inside arbitrary Ruby expressions, leading to an error or an incorrect result.

Good examples:
```ruby
count()
sum(_["x"])
sum(2 * _["x"])
sum(_["count"] * _["price"])
average(_.abs)
{total: sum(_["x"]), n: count()}
[count(), sum(_["x"])]
group_by(_["k"]) { {total: sum(_["x"]), n: count()} }
map_values { |v| sum(v) }
```

Bad examples:
```ruby
1 + count()            # use: count() >> _ + 1
2 * sum(_["x"])        # use: sum(2 * _["x"])
sum(_["x"]).round      # use: sum(_["x"]) >> _.round
[1 + count()]          # use: count() >> [_ + 1]
```

## RUBY LIBRARY

`jrf` can also be used as a Ruby library. Create a pipeline with `Jrf.new`, passing one or more procs as stages. The returned object is callable.

```ruby
require "jrf"

# Extract and filter
j = Jrf.new(
  proc { select(_["status"] == 200) },
  proc { _["path"] }
)
j.call(input_array)  # => ["/a", "/c", "/d"]

# Aggregate
j = Jrf.new(proc { {total: sum(_["price"]), n: count()} })
j.call(input_array)  # => [{total: 1250, n: 42}]

# Local variables are captured via closure
threshold = 10
j = Jrf.new(proc { select(_["x"] > threshold) })
```

Inside each proc, `_` is the current value and all built-in functions documented above are available.

The pipeline streams output when a block is given:

```ruby
j = Jrf.new(proc { _["id"] })
j.call(input_array) { |value| puts value }
```

## LICENSE

MIT
