# Compact BasicReport conversion

This tool converts local FULL BasicReport files to a draft COMPACT format and
reconstructs the original FULL protobuf. It does not contact a reporting service.

The compact format shares reporting units, windows, dimensions and qualification
filters. Typed numeric columns preserve every result value and row order.
The file-format version is 1. This draft is not the deployed Reporting API's
COMPACT view; its schema may change during review.

## Convert and verify

```shell
ConvertBasicReport --to=COMPACT --input=full.json --output=compact.json

ConvertBasicReport --to=FULL --input=compact.json --output=restored.json \
  --verify-against=full.json
```

The second command compares complete protobuf messages before writing the output.
A mismatch exits unsuccessfully and leaves the output file untouched. JSON
whitespace and omitted default values may differ even when the reports are equal.

`--input-format` and `--output-format` accept `JSON`, `TEXTPROTO` and
`BINARY`; both default to JSON. Input and output default to standard input and
standard output. The expected FULL file uses the input format.

```shell
ConvertBasicReport --to=FULL --input=compact.json \
  --output=restored.pb --output-format=BINARY
```

Unknown FULL protobuf fields are retained in compact files and reconstructed in
binary FULL output. JSON and textproto output reject any unencoded unknown fields
because those formats cannot reliably round-trip them. Binary output preserves
all unknown fields, including those in the report header and shared metadata.
Malformed compact dictionaries,
column lengths, message-presence bitmaps and unsupported format versions fail
before output is written.

Expansion defaults to a maximum of 1,000,000 result rows to bound allocation.
Use `--max-result-rows` to set an explicit budget.

## Complete example

[The FULL example](examples/full.textproto) and
[the COMPACT example](examples/compact.textproto) contain the same four weekly
California results for two publishers under AMI and MRC filters. All resources,
dates and values in these files are synthetic.

From this directory, reconstruct and verify the FULL example:

```shell
ConvertBasicReport --to=FULL --input=examples/compact.textproto \
  --input-format=TEXTPROTO --output-format=TEXTPROTO \
  --output=restored.textproto --verify-against=examples/full.textproto
```

The utility test also reads both committed files and compares their complete
protobuf messages.

## Library

Depend on
`//src/main/kotlin/org/wfanet/measurement/reporting/tools:basic_report_converter`.

```kotlin
val compact = BasicReportConverter.compact(full)
val restored = BasicReportConverter.expand(compact, maxResultRows = 100_000)
check(restored == full)
```

The utility preserves empty messages, scalar zero values, frequency-vector
lengths, negative counts, component ordering and unknown protobuf fields.
The preserved header contains the report's identity, specifications and state.

## Schema

See [the draft compact file proto](/src/main/proto/wfa/measurement/reporting/tools/compact_basic_report.proto).
Dictionary indices are one-based; zero denotes an absent metadata message or
an empty component name. An omitted scalar column represents zeros in every
row. Per-row message presence distinguishes empty messages from absent ones.
