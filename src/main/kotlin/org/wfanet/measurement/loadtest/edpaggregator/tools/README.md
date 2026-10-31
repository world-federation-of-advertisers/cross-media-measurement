# High overlap dataset specs

`GenerateHighOverlapSpecs` is the source of truth for the high overlap dataset.
The `high_overlap_*.textproto` specs are generated from it by a genrule
(`//src/main/proto/wfa/measurement/loadtest/dataprovider:BUILD.bazel`) and are
not checked in.

## If you change this generator

The dataset is seeded into an environment once, not per test run, by
`SeedHighOverlapDataset`. `EdpAggregatorReportingIntegrationTest` refuses to run
against a stale dataset: it compares the fingerprint of the generated specs
against the `spec_fingerprint` in the seeded manifest blob and fails if they
differ.

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

## Workflow configuration

Configure these GitHub environment variables before running Terraform or seeding
in `dev`, `qa`, or `head`:

| Variable | Value |
| --- | --- |
| `SECURE_COMPUTATION_STORAGE_BUCKET` | Existing Secure Computation bucket name; shared by Terraform, seeding and the integration tests. |
| `EDP7_KEK_URI` | Google Cloud KMS key URI shared by the Google Cloud EDPs. |
| `EDPA_META_KEK_URI` | AWS KMS key URI for `edpa_meta`. |
| `EDPA_META_AWS_ROLE_ARN` | AWS role used to access that key. |
| `EDPA_META_AWS_REGION` | AWS region containing that key. |

`SECURE_COMPUTATION_STORAGE_BUCKET` replaces `secure_computation_storage_bucket_name`
in the environment's `.tfvars` file. Use the existing bucket name when configuring
it. For local Terraform runs, pass `-var="secure_computation_storage_bucket_name=..."`.
The workflows fail before deployment or seeding if required configuration is missing.
