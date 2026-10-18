# High overlap dataset specs

`GenerateHighOverlapSpecs` is the source of truth for the high overlap dataset.
The `high_overlap_*.textproto` specs are generated from it by a genrule
(`//src/main/proto/wfa/measurement/loadtest/dataprovider:BUILD.bazel`) and are
not checked in.

## If you change this generator

The specs are seeded into an environment once, not per test run, and
`EdpAggregatorReportingIntegrationTest` refuses to run against a stale dataset:
it compares `HighOverlapDataset.fingerprint(config, populationSpec)` against the
`spec_fingerprint` in the seeded manifest blob and fails if they differ.

So after changing the generator, **re-seed every environment that runs the
reporting integration test**, or it will fail with
`Seeded dataset is stale for <model line>`.

```
gh workflow run high-overlap-seeding.yml --ref <branch> -f environment=dev
```

Environments: `dev`, `qa`, `head`. Seeding takes roughly 40 minutes and rewrites
the impression blobs, the manifest and the BigQuery ground-truth table.

`HighOverlapSpecsContractTest` guards the invariants the expected-value maths
relies on (VID ranges, stripe layout, demographic coverage). Run it before
seeding — it is cheap and catches a bad generator change without burning a
seeding run.
