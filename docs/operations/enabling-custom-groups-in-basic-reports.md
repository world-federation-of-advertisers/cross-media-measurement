# Enabling Custom Groups in BasicReports

How to enable custom-group (flexible) `BasicReport`s in a Reporting deployment,
and how a client creates one.

## Background

A `BasicReport` buckets its results by the components of each
`ResultGroupSpec.reporting_unit`. Historically those components could only be
`DataProvider` resource names: the client supplied a `campaign_group`
`ReportingSet`, the server minted one primitive `ReportingSet` per
`DataProvider` in it, and metrics came back keyed by `DataProvider`. This is
still the default mode and is unchanged.

Custom groups let a client bucket by its **own primitive `ReportingSet`s**
instead. The client puts `ReportingSet` resource names in
`reporting_unit.components`, leaves `campaign_group` empty, and the server
synthesizes the campaign group from the union of those `ReportingSet`s'
`EventGroup`s. The synthesized campaign group is materialized by a single
serializable get-or-create transaction, so concurrent and retried
`CreateBasicReport` calls — and later reports over the same `EventGroup`
universe — converge on one shared `ReportingSet` rather than minting
duplicates.

The synthesized campaign group is a real, Measurement Consumer-owned
`ReportingSet`: it is retrievable via `GetReportingSet`, appears in
`ListReportingSets`, and self-references as its own campaign group. It is
surfaced on the report in the `OUTPUT_ONLY` `effective_campaign_group` field;
the client-facing `campaign_group` stays empty.

The typical use case is slicing a single multi-publisher campaign by brand,
creative, or product category, where every slice spans the same publishers.

Custom groups do not change how a report is measured — only how results are
bucketed. All existing `DataProvider`-component reports continue to create,
read, and list unchanged.

## Enabling the feature

The feature is **off by default**. It is controlled by a flag on the Reporting
public API server (`V2AlphaPublicApiServer`):

```shell
--enable-reporting-set-reporting-unit-components=true
```

While the flag is `false`, a `CreateBasicReport` request with an empty
`campaign_group` is rejected with `INVALID_ARGUMENT`
(`basic_report.campaign_group must be specified when ReportingSet
ReportingUnits are not enabled`), i.e. the pre-feature behavior where
`campaign_group` was required.

Relevant declarations:

*   Flag definition:
    [`ReportingApiServerFlags.kt`](../../src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/ReportingApiServerFlags.kt)
*   Service wiring:
    [`V2AlphaPublicApiServer.kt`](../../src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/common/server/V2AlphaPublicApiServer.kt)

### Kubernetes

The flag is plumbed through the Reporting CUE configuration as the
`reporting_set_reporting_unit_components_enabled` build variable, so it is set
when the manifest is generated. Add it to the `bazel build` invocation you
already use to generate the Reporting manifest, as described in
[Deploying a Reporting Server](../gke/reporting-v2-server-deployment.md):

```shell
bazel build //src/main/k8s/dev:reporting_v2.tar \
  ... \
  --define basic_reports_enabled=true \
  --define reporting_set_reporting_unit_components_enabled=true \
  ...
```

Custom groups are a `BasicReport` feature, so `basic_reports_enabled` must also
be `true`.

The plumbing, for reference:

*   `build/variables.bzl` — `REPORTING_K8S_SETTINGS.reporting_set_reporting_unit_components_enabled`
*   `src/main/k8s/dev/BUILD.bazel` — passed as the
    `reporting_set_reporting_unit_components_enabled` CUE tag of the
    `reporting_v2_gke` `cue_dump`
*   `src/main/k8s/dev/reporting_v2_gke.cue` and
    `src/main/k8s/local/reporting_v2.cue` — read the tag
*   `src/main/k8s/reporting_v2.cue` — renders the server flag

Note that `docs/gke/reporting-v2-server-deployment.md` currently instructs
operators to set this variable to `false` pending release of the feature. Check
with the Halo team before turning it on in a production deployment.

### Database

The feature requires the Spanner `ALTER PROTO BUNDLE` migration that adds the
`ReportingSet`-keyed result types and the internal
`BasicReportDetails.campaign_group_synthesized` discriminator. It is registered
in the Reporting `changelog.yaml` and applied by the usual schema-update job.

There is **no table or column change and no data backfill**: the default
`campaign_group_synthesized = false` is exactly the legacy supplied-campaign-group
case, so existing rows are already correct.

## Creating a custom-group BasicReport

### Step 1 — create one primitive `ReportingSet` per slice

Each custom group must be a **primitive** `ReportingSet` (an explicit list of
`EventGroup`s) owned by the same Measurement Consumer as the report. Create them
with `CreateReportingSet` as usual. A slice's `EventGroup`s may span several
`DataProvider`s, and two slices may span the *same* set of `DataProvider`s —
they are still bucketed separately.

Composite `ReportingSet`s are rejected as components. A union-only composite is
equivalent to its flattened primitive, so this loses no capability.

### Step 2 — call `CreateBasicReport` with no campaign group

Omit `campaign_group` and list the `ReportingSet` resource names as the
components of every `result_group_specs` entry's `reporting_unit`:

```json
{
  "parent": "measurementConsumers/fB5zATKrslc",
  "basicReportId": "br-575a34a538c64542a35b2f53660b648d",
  "basicReport": {
    "title": "Custom Test 7",
    "reportingInterval": {
      "reportStart": {
        "year": 2021,
        "month": 3,
        "day": 15,
        "utcOffset": "0s"
      },
      "reportEnd": {
        "year": 2021,
        "month": 3,
        "day": 22
      }
    },
    "impressionQualificationFilters": [
      {
        "custom": {
          "filterSpec": [
            {
              "mediaType": "DISPLAY"
            }
          ]
        }
      }
    ],
    "resultGroupSpecs": [
      {
        "title": "Custom Test 7 (weekly)",
        "reportingUnit": {
          "components": [
            "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e",
            "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5"
          ]
        },
        "metricFrequency": {
          "weekly": "MONDAY"
        },
        "dimensionSpec": {},
        "resultGroupMetricSpec": {
          "populationSize": true,
          "reportingUnit": {
            "nonCumulative": {
              "reach": true,
              "averageFrequency": true,
              "impressions": true
            },
            "cumulative": {
              "reach": true
            }
          },
          "component": {
            "nonCumulative": {
              "reach": true,
              "averageFrequency": true,
              "impressions": true
            },
            "cumulative": {
              "reach": true
            },
            "nonCumulativeUnique": {
              "reach": true
            },
            "cumulativeUnique": {
              "reach": true
            }
          }
        }
      },
      {
        "title": "Custom Test 7 (total)",
        "reportingUnit": {
          "components": [
            "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e",
            "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5"
          ]
        },
        "metricFrequency": {
          "total": true
        },
        "dimensionSpec": {},
        "resultGroupMetricSpec": {
          "populationSize": true,
          "reportingUnit": {
            "cumulative": {
              "reach": true,
              "kPlusReach": 15,
              "averageFrequency": true,
              "impressions": true
            }
          },
          "component": {
            "cumulative": {
              "reach": true,
              "kPlusReach": 15,
              "averageFrequency": true,
              "impressions": true
            },
            "cumulativeUnique": {
              "reach": true
            }
          }
        }
      }
    ]
  }
}
```

Note that `campaign_group` is absent, and that both `result_group_specs`
entries use the same two `ReportingSet` components.

### Step 3 — read `effective_campaign_group`

The response (and every later `GetBasicReport`) carries the synthesized campaign
group in `effectiveCampaignGroup`. Creating a second report over the same set of
custom groups yields the *same* `effectiveCampaignGroup`.

### Validation rules

`CreateBasicReport` rejects a request with `INVALID_ARGUMENT` when:

*   Components mix `DataProvider` and `ReportingSet` names. A single
    `BasicReport` must be entirely one or the other.
*   A `ReportingSet` component is not primitive, or is not owned by the parent
    Measurement Consumer.
*   A `reporting_unit` has more than **25** components.

Additionally:

*   A missing `ReportingSet` component yields `FAILED_PRECONDITION`.
*   `ResultGroupMetricSpec.component_intersection` is not implemented for either
    mode and yields `UNIMPLEMENTED`.
*   Pairwise disjointness of the custom groups is **not** validated by the
    server; it is the client's responsibility. Overlapping groups will produce
    results, but the reporting-unit aggregates will reflect the overlap.

## Result shape

### Default mode: `DataProvider` components

Abridged `GetBasicReport` response for a conventional report. `campaignGroup`
is supplied by the client, `effectiveCampaignGroup` echoes it, and results are
keyed by `DataProvider`:

```json
{
  "name": "measurementConsumers/fB5zATKrslc/basicReports/basic-report-21221403-959a-4ccf-a53f-46028b7c5237",
  "title": "title",
  "campaignGroup": "measurementConsumers/fB5zATKrslc/reportingSets/a-21221403-959a-4ccf-a53f-46028b7c5237",
  "effectiveCampaignGroup": "measurementConsumers/fB5zATKrslc/reportingSets/a-21221403-959a-4ccf-a53f-46028b7c5237",
  "state": "SUCCEEDED",
  "createTime": "2026-08-13T13:05:49.269679Z",
  "reportingInterval": {
    "reportStart": {
      "year": 2021,
      "month": 3,
      "day": 14,
      "hours": 20,
      "timeZone": {
        "id": "America/New_York"
      }
    },
    "reportEnd": {
      "year": 2021,
      "month": 3,
      "day": 15
    }
  },
  "modelLine": "modelProviders/eaaPUbwUC5c/modelSuites/NMtDnLwcnNo/modelLines/PFUW1Lwcnyo",
  "effectiveModelLine": "modelProviders/eaaPUbwUC5c/modelSuites/NMtDnLwcnNo/modelLines/PFUW1Lwcnyo",
  "resultGroupSpecs": [
    {
      "title": "title",
      "reportingUnit": {
        "components": [
          "dataProviders/AYTWfc1UUY0"
        ]
      },
      "metricFrequency": {
        "weekly": "MONDAY"
      },
      "dimensionSpec": {},
      "resultGroupMetricSpec": {
        "populationSize": true,
        "component": {
          "nonCumulative": {
            "reach": true,
            "kPlusReach": 5,
            "impressions": true
          }
        }
      }
    }
  ],
  "resultGroups": [
    {
      "title": "title",
      "results": [
        {
          "metadata": {
            "reportingUnitSummary": {
              "reportingUnitComponentSummary": [
                {
                  "component": "dataProviders/AYTWfc1UUY0",
                  "eventGroupSummaries": [
                    {
                      "eventGroup": "measurementConsumers/fB5zATKrslc/eventGroups/O_wKD3g14Ss"
                    }
                  ]
                }
              ]
            },
            "nonCumulativeMetricStartTime": "2021-03-15T00:00:00Z",
            "cumulativeMetricStartTime": "2021-03-15T00:00:00Z",
            "metricEndTime": "2021-03-16T00:00:00Z",
            "metricFrequency": {
              "weekly": "MONDAY"
            },
            "dimensionSpecSummary": {},
            "filter": {
              "impressionQualificationFilter": "impressionQualificationFilters/mrc"
            }
          },
          "metricSet": {
            "populationSize": 34288880,
            "reportingUnit": {},
            "components": [
              {
                "key": "dataProviders/AYTWfc1UUY0",
                "value": {
                  "nonCumulative": {
                    "reach": 71717,
                    "kPlusReach": [
                      71717,
                      70270,
                      46738,
                      46738,
                      30938
                    ],
                    "impressions": 325420
                  }
                }
              }
            ]
          }
        }
      ]
    }
  ]
}
```

### Custom-group mode: `ReportingSet` components

Abridged `GetBasicReport` response for the request in Step 2. Three differences
matter:

1.  `campaignGroup` is **absent**; `effectiveCampaignGroup` names the
    server-synthesized `ReportingSet`.
2.  `reportingUnitComponentSummary[].component` and `metricSet.components[].key`
    are `ReportingSet` resource names, not `DataProvider` names.
3.  `metricSet.reportingUnit` carries the aggregate across the union of all
    custom groups, while each entry in `metricSet.components` carries that
    slice's own metrics plus its `nonCumulativeUnique` / `cumulativeUnique`
    contribution.

```json
{
  "name": "measurementConsumers/fB5zATKrslc/basicReports/br-575a34a538c64542a35b2f53660b648d",
  "title": "Custom Test 7",
  "effectiveCampaignGroup": "measurementConsumers/fB5zATKrslc/reportingSets/aae7e09a3-94df-4044-a05c-d95e6f3e0c7b",
  "state": "SUCCEEDED",
  "createTime": "2026-07-17T17:13:10.601087Z",
  "reportingInterval": {
    "reportStart": {
      "year": 2021,
      "month": 3,
      "day": 15,
      "utcOffset": "0s"
    },
    "reportEnd": {
      "year": 2021,
      "month": 3,
      "day": 22
    }
  },
  "impressionQualificationFilters": [
    {
      "custom": {
        "filterSpec": [
          {
            "mediaType": "DISPLAY"
          }
        ]
      }
    }
  ],
  "effectiveImpressionQualificationFilters": [
    {
      "impressionQualificationFilter": "impressionQualificationFilters/ami"
    },
    {
      "impressionQualificationFilter": "impressionQualificationFilters/mrc"
    },
    {
      "custom": {
        "filterSpec": [
          {
            "mediaType": "DISPLAY"
          }
        ]
      }
    }
  ],
  "effectiveModelLine": "modelProviders/eaaPUbwUC5c/modelSuites/NMtDnLwcnNo/modelLines/PFUW1Lwcnyo",
  "resultGroups": [
    {
      "title": "Custom Test 7 (weekly)",
      "results": [
        {
          "metadata": {
            "reportingUnitSummary": {
              "reportingUnitComponentSummary": [
                {
                  "component": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e"
                },
                {
                  "component": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5"
                }
              ]
            },
            "nonCumulativeMetricStartTime": "2021-03-15T00:00:00Z",
            "cumulativeMetricStartTime": "2021-03-15T00:00:00Z",
            "metricEndTime": "2021-03-22T00:00:00Z",
            "metricFrequency": {
              "weekly": "MONDAY"
            },
            "dimensionSpecSummary": {},
            "filter": {
              "custom": {
                "filterSpec": [
                  {
                    "mediaType": "DISPLAY"
                  }
                ]
              }
            }
          },
          "metricSet": {
            "populationSize": 34288880,
            "reportingUnit": {
              "nonCumulative": {
                "reach": 54316,
                "averageFrequency": 28.467855,
                "impressions": 1546260
              },
              "cumulative": {}
            },
            "components": [
              {
                "key": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e",
                "value": {
                  "nonCumulative": {
                    "reach": 54316,
                    "averageFrequency": 1,
                    "impressions": 54316
                  },
                  "cumulative": {},
                  "nonCumulativeUnique": {},
                  "cumulativeUnique": {}
                }
              },
              {
                "key": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5",
                "value": {
                  "nonCumulative": {
                    "reach": 54316,
                    "averageFrequency": 27.467855,
                    "impressions": 1491944
                  },
                  "cumulative": {},
                  "nonCumulativeUnique": {},
                  "cumulativeUnique": {}
                }
              }
            ]
          }
        }
      ]
    },
    {
      "title": "Custom Test 7 (total)",
      "results": [
        {
          "metadata": {
            "reportingUnitSummary": {
              "reportingUnitComponentSummary": [
                {
                  "component": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e"
                },
                {
                  "component": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5"
                }
              ]
            },
            "nonCumulativeMetricStartTime": "1970-01-01T00:00:00Z",
            "cumulativeMetricStartTime": "2021-03-15T00:00:00Z",
            "metricEndTime": "2021-03-22T00:00:00Z",
            "metricFrequency": {
              "total": true
            },
            "dimensionSpecSummary": {},
            "filter": {
              "impressionQualificationFilter": "impressionQualificationFilters/ami"
            }
          },
          "metricSet": {
            "populationSize": 34288880,
            "reportingUnit": {
              "cumulative": {
                "kPlusReach": [
                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                ],
                "impressions": 2447637
              }
            },
            "components": [
              {
                "key": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-3b2ed060a549a531600113c0b21033dd2b01d7bebff0a16d7e",
                "value": {
                  "cumulative": {
                    "kPlusReach": [
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                    ],
                    "impressions": 921084
                  },
                  "cumulativeUnique": {}
                }
              },
              {
                "key": "measurementConsumers/fB5zATKrslc/reportingSets/primitive-c1528f0a6a11d2c5b8e74763b6095a47d361ebf18dcfc2a1f5",
                "value": {
                  "cumulative": {
                    "kPlusReach": [
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                    ],
                    "impressions": 1526553
                  },
                  "cumulativeUnique": {}
                }
              }
            ]
          }
        }
      ]
    }
  ]
}
```

Both examples are abridged: each `results` list in a real response contains one
entry per `(impression qualification filter, metric window)` combination, so a
full response repeats the structure above many times.

## Reference tests

The tests below are the executable specification of the client contract and are
the best source for exact request shapes.

Unit tests:

*   [`BasicReportsServiceTest.kt`](../../src/test/kotlin/org/wfanet/measurement/reporting/service/api/v2alpha/BasicReportsServiceTest.kt)
    — the helpers `createPrimitiveReportingSet` and
    `reportingSetComponentBasicReportRequest` build the canonical request. Cases
    cover campaign-group synthesis, reuse of the synthesized campaign group
    across two reports, rejection when the feature flag is disabled, rejection
    of mixed `DataProvider`/`ReportingSet` components, rejection of a composite
    component, a missing component, and distinct bucketing for two
    `ReportingSet`s that share a `DataProvider` set.
*   [`BasicReportProtoConversionsTest.kt`](../../src/test/kotlin/org/wfanet/measurement/reporting/service/api/v2alpha/BasicReportProtoConversionsTest.kt)
    — `campaign_group` / `effective_campaign_group` derivation and the
    `ReportingSet`-keyed read path.
*   [`BasicReportProcessedResultsTransformationTest.kt`](../../src/test/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/spanner/BasicReportProcessedResultsTransformationTest.kt)
    — the `ReportingSet`-keyed result write path.
*   [`ReportingSetsServiceTest.kt`](../../src/main/kotlin/org/wfanet/measurement/reporting/service/internal/testing/v2/ReportingSetsServiceTest.kt)
    — `EnsureSynthesizedCampaignGroupReportingSet` get-or-create semantics:
    create-new, reuse for the same `EventGroup` set, order-independent reuse,
    and distinctness against a superset.

Integration tests (closest to a real client, end to end):

*   [`InProcessEdpAggregatorMultiEdpReportTest.kt`](../../src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorMultiEdpReportTest.kt)
    — `no noise basic report with ReportingSet components has the expected
    result` creates one primitive `ReportingSet` per EDP, creates the report
    with an empty campaign group, and asserts the numeric results.
*   [`InProcessMultiEdpReportIntegrationTest.kt`](../../src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessMultiEdpReportIntegrationTest.kt)
    — `getBasicReport returns SUCCEEDED ReportingSet basic report when basic
    report is completed` covers the create/poll/read round trip.
*   [`InProcessReportingServer.kt`](../../src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessReportingServer.kt)
    — shows the in-process server enabling the feature flag.

## Operational note

`BasicReport`s written directly through the internal `InsertBasicReport` method
(bypassing the public create path) before that method validated
`ReportingUnitComponentSummary.external_reporting_set_id` can have that field
unset. The read path used to assemble a `ReportingSet` resource name from it
unconditionally, so a single affected record made `GetBasicReport` fail and
failed the whole page for `ListBasicReports`.

The current code validates the field at insert time and tolerates it being
unset on the read path. Pre-existing affected rows are repaired with the
`BackfillBasicReportReportingSets` tool, which sweeps every `SUCCEEDED`
`BasicReport` across all Measurement Consumers, resolves each affected component
summary to the campaign group child `ReportingSet` whose membership equals the
component's `event_group_summaries` (creating one where no match exists), and
supports `--dry-run` to report without writing. The tool is idempotent.
