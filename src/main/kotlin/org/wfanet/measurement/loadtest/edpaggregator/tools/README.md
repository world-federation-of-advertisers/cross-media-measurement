# High overlap dataset specs

The population spec is committed at
`src/main/proto/wfa/measurement/api/v2alpha/360m_population_spec_us.textproto`
and is read directly by tests and seeding. `GenerateHighOverlapSpecs` generates
the impression specs and config through the genrule in that package.

## If you change the fixture

The dataset is seeded into an environment once, not per test run, by
`SeedHighOverlapDataset`. `EdpAggregatorReportingIntegrationTest` compares the
fingerprint of the population and impression specs with `spec_fingerprint` in
the seeded manifest and refuses to run against a stale dataset.

Changing reached VIDs, their demographics or impression specs requires reseeding
every environment that runs the reporting integration test. Changing only the
unreached population preserves existing impressions, but the Population,
ModelRelease/ModelRollout, population-spec blob and manifest must be updated
before running the test.

The seeding workflow performs a full rebuild, including impressions. Use it for
changes that require reseeding; updating only population resources and the
manifest must be done separately to reuse existing impressions.

```
gh workflow run high-overlap-seeding.yml --ref <branch> -f environment=dev
```

Environments: `dev`, `qa`, `head`. Seeding takes roughly 40 minutes and rewrites
the impression blobs, the manifest and the BigQuery ground-truth table.

`HighOverlapSpecsContractTest` guards VID ranges, stripe layout and demographic
coverage. Run it before seeding to catch fixture errors early.

## Workflow configuration

Configure these values in `.github/env-configs/<environment>/cmms.json` for `dev`,
`qa`, and `head`. The seeding workflow loads them with
`.github/actions/load-cmms-environment`:

| Variable | Value |
| --- | --- |
| `SECURE_COMPUTATION_STORAGE_BUCKET` | Existing Secure Computation bucket name; must match `secure_computation_storage_bucket_name` in the environment's `.tfvars` file. |
| `EDP7_KEK_URI` | Google Cloud KMS key URI shared by the Google Cloud EDPs. |
| `EDPA_META_KEK_URI` | AWS KMS key URI for `edpa_meta`. |
| `EDPA_META_AWS_ROLE_ARN` | AWS role used to access that key. |
| `EDPA_META_AWS_REGION` | AWS region containing that key. |

Terraform continues to read `secure_computation_storage_bucket_name` from the
environment's `.tfvars` file. The test workflows use the
`secure-computation-storage-<environment>-bucket` naming pattern. Keep the JSON
bucket value aligned with both. No GitHub environment variables are needed for
these settings.
