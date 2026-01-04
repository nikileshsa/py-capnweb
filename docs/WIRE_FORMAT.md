# CapnWeb Wire Format - Parsing Algorithm

This document describes how the TypeScript capnweb library handles wire format parsing
and how the Python implementation should match it.

## Overview

The wire format uses JSON arrays with a string tag as the first element to represent
special values. The key insight is that **regular application arrays are ESCAPED** by
wrapping them in an outer single-element array.

## Serialization (Devaluator)

When serializing values for transmission:

1. **Primitives** (null, bool, int, float, string): Pass through unchanged
2. **Objects/Dicts**: Recursively serialize each value
3. **Arrays**: **ALWAYS wrap in outer array** → `[original_array]`
4. **Special types**: Encode as tagged arrays:
   - `["bigint", "123"]` - BigInt as string
   - `["date", 1234567890]` - Date as timestamp (number)
   - `["bytes", "base64..."]` - Binary as base64 string
   - `["error", "TypeError", "message", "stack?"]` - Error
   - `["undefined"]` - undefined value
   - `["inf"]`, `["-inf"]`, `["nan"]` - Special numbers
   - `["export", id]` - Exporting a capability (id is number)
   - `["promise", id]` - Exporting a promise (id is number)
   - `["import", id]` - Referencing peer's export (id is number)
   - `["pipeline", id, path?, args?]` - Pipelining on peer's export (id is number)
   - `["remap", id, path, captures, instructions]` - Map operation

## Deserialization (Evaluator)

The `evaluateImpl` function in TypeScript:

```
evaluateImpl(value):
  if value is Array:
    if len == 1 AND value[0] is Array:
      # ESCAPED ARRAY - unwrap and recursively evaluate contents
      return [evaluateImpl(item) for item in value[0]]
    
    # Check for special forms by first element
    switch value[0]:
      case "bigint": if value[1] is string → return BigInt(value[1])
      case "date": if value[1] is number → return Date(value[1])
      case "bytes": if value[1] is string → return decode_base64(value[1])
      case "error": if len >= 3 AND value[1] is string AND value[2] is string → return Error
      case "undefined": if len == 1 → return undefined
      case "inf": return Infinity
      case "-inf": return -Infinity
      case "nan": return NaN
      
      case "import":
      case "pipeline":
        if len < 2 OR len > 4: break  # fall through to error
        if value[1] is NOT number: break  # fall through to error
        # ... handle import/pipeline
        
      case "remap":
        if len != 5 OR value[1] is NOT number: break
        # ... handle remap
        
      case "export":
      case "promise":
        if value[1] is number:
          # ... handle export/promise
        break
    
    # If we get here, it's an unknown special value
    throw TypeError("unknown special value: ...")
    
  else if value is Object:
    return {key: evaluateImpl(val) for key, val in value}
  
  else:
    # Primitives pass through
    return value
```

## Key Design Principles

1. **All application arrays are escaped**: When you serialize `[1, 2, 3]`, it becomes
   `[[1, 2, 3]]` on the wire. This means any unescaped array MUST be a special form.

2. **Type validation before parsing**: Each special form validates its structure
   (length, types of elements) BEFORE treating it as that form. If validation fails,
   it falls through to throw an error.

3. **No ambiguity**: Because application arrays are escaped, there's no ambiguity
   between `["pipeline", {...}]` (application data) and `["pipeline", 0, ...]` 
   (wire expression). The former would be serialized as `[["pipeline", {...}]]`.

4. **Strict error handling**: If an array starts with a known tag but doesn't match
   the expected structure, it throws an error rather than treating it as data.

## Python Implementation

### wire.py - `wire_expression_from_json`

Converts JSON wire format to Python wire expression types:
- Unescaped arrays with known tags → special forms
- Escaped arrays `[[...]]` → unwrapped to application arrays
- Unknown tags → treated as regular arrays (lenient parsing)

### parser.py - `Parser._parse_value`

The Python equivalent of TypeScript's `Evaluator.evaluateImpl`:
1. Handles escaped arrays by unwrapping
2. Validates special form structure before parsing
3. Converts wire types to Python types (e.g., `WireDate` → `datetime`)

## Wire Expression Types Summary

| Tag | Structure | value[1] type | Notes |
|-----|-----------|---------------|-------|
| `bigint` | `["bigint", str]` | string | |
| `date` | `["date", num]` | number | |
| `bytes` | `["bytes", str]` | string | base64 |
| `error` | `["error", name, msg, stack?]` | string | |
| `undefined` | `["undefined"]` | N/A | |
| `inf` | `["inf"]` | N/A | |
| `-inf` | `["-inf"]` | N/A | |
| `nan` | `["nan"]` | N/A | |
| `export` | `["export", id]` | number | |
| `promise` | `["promise", id]` | number | |
| `import` | `["import", id, path?, args?]` | number | |
| `pipeline` | `["pipeline", id, path?, args?]` | number | |
| `remap` | `["remap", id, path, captures, instr]` | number | len == 5 |
