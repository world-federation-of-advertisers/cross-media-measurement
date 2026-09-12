# QA Synthetic Data Shape

## What Is This?

This describes the shape of the QA synthetic data seeded for the EDP-aggregator
path, and — more importantly — *why* each part of it is sized the way it is. The
spec files encode the what; this encodes the reasoning, so that the next person to
regenerate the data preserves the properties the reports depend on.

The specs themselves are generated. Do not hand-edit them:

```shell
python3 src/main/python/wfa/measurement/loadtest/qa2026/generate_specs.py
```

That writes, into `src/main/proto/wfa/measurement/loadtest/dataprovider/`:

*   `qa2026_population_spec.textproto` — the `PopulationSpec`
*   `qa2026_seg_<name>.textproto` — one `SyntheticEventGroupSpec` per segment
*   `qa2026_impression_test_data_config.textproto` — the `ImpressionTestDataConfig`

## The Noise Floor, and Why It Sets Everything

Every reach measurement has noise added with a **fixed standard deviation**, in
people. It does not shrink as the audience grows, so the relative error of a
measurement is `1.96σ / reach`. This single fact drives the sizing.

σ comes from the privacy parameters in
`src/main/k8s/testing/secretfiles/basic_report_metric_spec_config.textproto`,
amplified by the VID sampling width:

```
σ_full = sqrt(2 · ln(1.25/δ)) / (ε · sampling_width)
```

| Metric | ε | sampling width | σ |
| ------------ | ----- | -------------- | ------ |
| reach-only   | 0.041 | 0.01           | 18,204 |
| reach in R&F | 0.033 | 1/60           | 13,571 |

The **sampling width dominates**: reach is measured over 1% of the VID space and
extrapolated, so the noise is amplified a hundredfold. The sampling intervals are
a deliberate disjoint partition of `[0, 1]` — reach `[0, 0.01]`, R&F
`[0.16, 0.1767]`, impressions `[0.4767, 0.6833]`, watch duration
`[0.6833, 1.0]` — so that metrics do not reuse the same VIDs. **Do not widen
one without re-cutting the partition.**

The ε values above are 10× the original ones. At the original ε, σ was 182,044
and the aggregator EDPs' true reach was in the low tens of thousands, so every
reach measurement was noise-dominated and the post-processor zeroed it. Two
levers close that gap — more reach, and less noise — and this dataset uses both,
because automated tests report over a 1–2 week window rather than the full
90-day flight, which cuts the reach available to any single report.

Note this file is shared across dev, qa and head. There is no environment-scoped
override.

## Reached Population

`qa2026_population_spec.textproto` declares **360,000,000** VIDs, of which
**10,610,000** are reachable.

The full 360M is not reached; the remainder is a single filler subpopulation that
generates no impressions. It exists because the frequency vector is a dense
`ByteArray` sized to the whole population — one slot per population VID, per
filter sink — so a 360M population stresses frequency-vector allocation
regardless of how few VIDs are actually reached.

### Demographic Striping

The reached range is divided into **10,000-VID stripes** cycling round-robin
through the 30 `(gender × age_group × us_state)` tuples of
`event_templates.testing.v1.Common`:

*   gender — `MALE`, `FEMALE`
*   age_group — `YEARS_18_TO_34`, `YEARS_35_TO_54`, `YEARS_55_PLUS`
*   us_state — `CALIFORNIA`, `NEW_YORK`, `TEXAS`, `FLORIDA`, `ILLINOIS`

Striping exists so demographics stay **orthogonal to the Venn structure**.
Demographics partition reach — a VID has exactly one gender, one age band, one
state — whereas Venn membership comes only from which EDPs carry a segment. One
full cycle is `30 × 10,000 = 300,000` VIDs, which fits inside the *smallest*
Venn region, so every region carries the full demographic mix and any region can
be broken down by gender, age or state.

A coarse spec would defeat this. The pre-existing `360m_population_spec` has 12
subpopulations of 30M each, so all of VIDs 1–10.6M fall inside a single
`MALE / YEARS_18_TO_34 / CALIFORNIA` subpopulation — zero demographic variation
across the entire reached range.

## Overlap Topology

Four aggregator EDPs: `edp7`, `edpa_meta`, `edpa_video_pub`, `edpa_retail_pub`.

The reached VID space is partitioned into one **segment per primitive Venn
region**. A segment is seeded on exactly the EDPs named in its region, so
selecting the whole menu realizes all 15 regions at once. Overlap is not baked
into the data — each EDP has its own impression blobs — it emerges from the fact
that two EDPs seeded the same VID range.

| Segment | VID range | Size |
| ------------------- | ----------------------- | ---- |
| e7only              | 1 – 2,000,000           | 2.0M |
| metaonly            | 2,000,001 – 3,600,000   | 1.6M |
| videoonly           | 3,600,001 – 4,800,000   | 1.2M |
| retailonly          | 4,800,001 – 5,800,000   | 1.0M |
| e7 ∩ meta           | 5,800,001 – 6,600,000   | 0.8M |
| e7 ∩ video          | 6,600,001 – 7,200,000   | 0.6M |
| e7 ∩ retail         | 7,200,001 – 7,600,000   | 0.4M |
| meta ∩ video        | 7,600,001 – 8,100,000   | 0.5M |
| meta ∩ retail       | 8,100,001 – 8,400,000   | 0.3M |
| video ∩ retail      | 8,400,001 – 8,800,000   | 0.4M |
| e7 ∩ meta ∩ video   | 8,800,001 – 9,300,000   | 0.5M |
| e7 ∩ meta ∩ retail  | 9,300,001 – 9,600,000   | 0.3M |
| e7 ∩ video ∩ retail | 9,600,001 – 9,900,000   | 0.3M |
| meta ∩ video ∩ retail | 9,900,001 – 10,300,000 | 0.4M |
| all four            | 10,300,001 – 10,600,000 | 0.3M |
| noise probe         | 10,600,001 – 10,608,000 | 8k   |

Resulting per-EDP reach, and its margin over σ:

| EDP | Reach | σ multiple | Tolerance (95% CI) |
| ----------------- | --------- | ---------- | ------------------ |
| `edp7`            | 5,208,000 | 286σ       | ±0.7% |
| `edpa_meta`       | 4,700,000 | 258σ       | ±0.8% |
| `edpa_video_pub`  | 4,200,000 | 231σ       | ±0.8% |
| `edpa_retail_pub` | 3,400,000 | 187σ       | ±1.0% |

Because VID ranges are deterministic, expected reach, frequency and overlap are
computable exactly — tests can assert values, not just shapes. Selecting
*non*-overlapping segments drives chosen regions to exactly zero, which verifies
that empty regions stay empty.

### The Noise Probe

One segment is deliberately **sub-σ**: 8,000 VIDs on a single EDP, outside the 15
regions, at roughly **0.44σ**. It exists so noise-dominated reports get flagged or
FAILED rather than delivered with silently zeroed reach, while every other report
proves clean data passes through unchanged.

**If ε changes, this segment must be resized.** It was originally specified at
~60k against σ ≈ 120k. At the current σ ≈ 18k, 60k would be 3.3σ — no longer
noise-dominated, and the guard it exists to exercise would stop firing.

## Time Shape and Frequency

The window is **static**: 2026-04-01 → 2026-06-30. It does not roll. A frozen
window keeps expected values hardcodable; the cost is that the data ages and the
window needs re-pinning eventually.

Segments run on one of three flights, so the mix changes over the 90 days and
whole-campaign and per-week identities differ:

*   spring — Apr 1 – May 15
*   summer — May 15 – Jun 30
*   always-on — Apr 1 – Jun 30

Within a flight, each segment's VID range is split into **6 tranches** whose date
ranges start progressively later but all run to the end of the flight. This
staggers first exposure, so cumulative reach grows week over week instead of
saturating on day one.

Each tranche is then split by frequency, giving a non-flat, monotonic K+ curve:

| Frequency | Share |
| --------- | ----- |
| 1         | 40%   |
| 2         | 30%   |
| 3         | 20%   |
| 5         | 10%   |

Mean frequency is **2.1**, for **~36.8M impressions** total across the four EDPs.

## Media Types and Engagement

Media type is carried by the event template itself — populating `video.*` paths
generates `VIDEO` events, `display.*` paths generate `DISPLAY`. Each tranche is
split so that **every segment, and therefore every EDP, emits both**.

This is deliberate: media type is *not* aligned with EDP or Venn structure, so a
VID shared across EDPs can be reached by a video impression on one publisher and
a display impression on another. Their union is then genuine cross-media reach,
which is the deduplication the system exists to perform. Siloed VIDEO-only and
DISPLAY-only reports would not exercise it.

Engagement values are cycled per block so threshold impression-qualification
filters select real, distinct subsets:

*   `video.completed_fraction` ∈ {0, 0.25, 0.5, 0.75, 1}
*   `video.viewable_fraction`, `display.viewable_fraction` ∈ {0, 0.5, 1}

## Event Groups

`qa2026_impression_test_data_config.textproto` declares one event group per
**(segment, EDP)** pair — 33 in total — spread across `campaign`, `ad_group` and
`creative-id` entity key types, including multi-entity groups.

**Every event group must carry an entity key.** `EventGroupSync` filters its
Kingdom listing by entity type when `entity_key_types` is configured, so an event
group whose type is absent from that list is invisible to the existence check and
gets **created again on every sync**, silently duplicating the reference ID. The
configured types live in the `EVENT_GROUP_SYNC_CONFIG_CONTENT` environment
variable and must include every type used here.

## Pre-Labeled, Not Pipelined

All four EDPs' impressions are written **already VID-labeled**. No date is routed
through the deployed VID labeling pipeline.

This is a deliberate simplification. VIDs come straight from the population spec,
so expected reach is exact rather than the output of a hash that must be replayed
offline and that has birthday collisions by construction. It also means no VID
model needs provisioning for this dataset, the memoized rank-index path and its
retention constraints do not apply, and pipeline Phases 0 and 1 never run.

The two trigger paths stay separate: `done` markers under the raw-impressions
prefix drive the VID labeling dispatcher, while `done` markers alongside labeled
impressions drive `DataAvailabilitySync`. Writing only the latter never wakes the
labeling pipeline.

## Which Test Owns This, and Why

Two correctness tests run against a deployed environment, and they exercise
**different data-delivery paths**. This dataset belongs to exactly one of them.

| | `SyntheticGeneratorCorrectnessTest` | `EdpAggregatorCorrectnessTest` |
| --- | --- | --- |
| EDPs | classic simulators `edp1`–`edp6` | aggregator `edp7`, `edpa_meta`, … |
| Data delivery | generated **in-process** by `SyntheticGeneratorEventQuery` from a spec; nothing is stored | encrypted **blobs in GCS**, read by the results fulfiller |
| Owns this dataset | no | **yes** |

The 2026 data is blob-delivered, so the simulator test has no mechanism to
consume it — those EDPs compute events on demand rather than reading storage.
This is also a deliberate scope boundary: expanding the classic simulators'
volume or date coverage is explicitly out of scope, because the aggregator path
is the one production depends on and the one that could not be validated.

Within `EdpAggregatorCorrectnessTest` the QA 2026 rules are **additive**. The
2021 fixture keeps its own config, population spec, model line, dates and event
group reference IDs, and its assertions are untouched. Every QA 2026 rule no-ops
unless `QA2026_MODEL_LINE` is set, so an environment opts in only once its
ModelLine has been provisioned.

## Model Line

The 2026 data uses its **own** `ModelLine` and its own Kingdom `Population`.

A `Population` is resolved per model line — the PDP walks ModelLine → latest
ModelRollout → ModelRelease → Population — so sharing a line with the 2021
fixture would replace that fixture's population too. Impressions are also stored
under `model-line/<modelLineId>/<date>/`, so a separate line keeps the two
datasets in separate directory trees.

### Provisioning It

`active_start_time` is bounded on **both** sides:

*   It must be at or before the earliest event date, or the correctness test's
    active-window check fails fast.
*   It must stay **after the 2021 fixture's dates**. The VID labeling dispatcher
    decides which model lines to process purely from
    `[active_start_time, active_end_time)`, so a 2026 line active in March 2021
    would also be dispatched for the 2021 raw-impression upload. That line has no
    VID model blob by design, so the work could never finish and
    `AwaitVidLabelingRule` would time out — breaking the 2021 test.

`2026-01-01` satisfies both with room to spare.

Nothing in this repository creates a `ModelLine`; every `ensureModelLine` is a
lookup that fails if absent. So the line is provisioned once per environment by
an operator, using the `ModelRepository` tool, and referenced by resource name.

`model-lines create` requires `--population`, because it creates the ModelLine, a
`ModelRelease` and a `ModelRollout` together. Bootstrap it against any existing
Population: `Qa2026ModelResourcesRule` then creates the correct 2026 Population
and attaches its own release and rollout, which supersedes the bootstrap because
the PDP resolves the **most recent** rollout on a line.

That accumulation is the intended design, not a workaround — a line collects
rollouts over time and the newest is live. The same pattern already runs in
`SyntheticGeneratorCorrectnessTest`, whose `ensurePopulation` and
`ensureModelRelease` this rule mirrors. Those two are wired to the 2021 spec and
the 2021 line, which is why the logic is repeated here rather than reused.

The Population only affects **population measurements** (`population_size`).
Reach, frequency and impressions come from the impression data, so an
unattached Population makes `population_size` wrong and nothing else.

## See Also

*   [Deployment Guide](deployment-guide.md) — seeding and `--create-done-blobs`
*   [EDP Onboarding](edp-onboarding.md) — registering a new aggregator EDP
