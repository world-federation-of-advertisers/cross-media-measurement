# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generates the QA 2026 synthetic data specs.

Emits, into src/main/proto/wfa/measurement/loadtest/dataprovider/:

  * qa2026_population_spec.textproto   - PopulationSpec, 360M VIDs
  * qa2026_seg_<name>.textproto        - SyntheticEventGroupSpec, one per segment
  * qa2026_impression_test_data_config.textproto - ImpressionTestDataConfig

The reached VID space is partitioned into one segment per primitive Venn region
across four aggregator EDPs, plus a deliberately sub-sigma noise probe. A segment
is seeded on exactly the EDPs named in its region, so selecting the whole menu
lights up all 15 regions at once.

Demographics are orthogonal to the Venn structure: the 30 (gender, age_group,
us_state) tuples are interleaved in 10,000-VID stripes cycling round-robin, so one
full cycle (300k VIDs) fits inside the smallest region and every region carries the
full demographic mix.

Run:
  python3 src/main/python/wfa/measurement/loadtest/qa2026/generate_specs.py
"""

import datetime
import os
import pathlib

# --- Population -------------------------------------------------------------

GENDERS = ["MALE", "FEMALE"]
AGE_GROUPS = ["YEARS_18_TO_34", "YEARS_35_TO_54", "YEARS_55_PLUS"]
US_STATES = ["CALIFORNIA", "NEW_YORK", "TEXAS", "FLORIDA", "ILLINOIS"]

STRIPE_SIZE = 10_000
TOTAL_POPULATION = 360_000_000

COMMON_TYPE_URL = (
    "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.v1.Common"
)

# --- Segments ---------------------------------------------------------------

EDP7 = "edp7"
META = "edpa_meta"
VIDEO = "edpa_video_pub"
RETAIL = "edpa_retail_pub"

FLIGHT_SPRING = (datetime.date(2026, 4, 1), datetime.date(2026, 5, 15))
FLIGHT_SUMMER = (datetime.date(2026, 5, 15), datetime.date(2026, 7, 1))
FLIGHT_ALWAYS_ON = (datetime.date(2026, 4, 1), datetime.date(2026, 7, 1))

# name, vid_start, vid_count, edps, entity_type, entity_count, flight, metadata
SEGMENTS = [
    ("e7only", 1, 2_000_000, [EDP7], "campaign", 1, FLIGHT_ALWAYS_ON,
     ("brand-a", "always-on", "feed")),
    ("metaonly", 2_000_001, 1_600_000, [META], "ad_group", 3, FLIGHT_SPRING,
     ("brand-a", "spring-launch", "instream")),
    ("videoonly", 3_600_001, 1_200_000, [VIDEO], "creative-id", 3, FLIGHT_SPRING,
     ("brand-a", "spring-launch", "homepage")),
    ("retailonly", 4_800_001, 1_000_000, [RETAIL], "campaign", 1, FLIGHT_SUMMER,
     ("brand-a", "summer-sale", "feed")),
    ("e7-meta", 5_800_001, 800_000, [EDP7, META], "ad_group", 1, FLIGHT_SPRING,
     ("brand-b", "spring-launch", "feed")),
    ("e7-video", 6_600_001, 600_000, [EDP7, VIDEO], "creative-id", 2, FLIGHT_ALWAYS_ON,
     ("brand-b", "always-on", "homepage")),
    ("e7-retail", 7_200_001, 400_000, [EDP7, RETAIL], "campaign", 1, FLIGHT_SUMMER,
     ("brand-b", "summer-sale", "feed")),
    ("meta-video", 7_600_001, 500_000, [META, VIDEO], "ad_group", 3, FLIGHT_SPRING,
     ("brand-a", "spring-launch", "instream")),
    ("meta-retail", 8_100_001, 300_000, [META, RETAIL], "campaign", 1, FLIGHT_SUMMER,
     ("brand-b", "summer-sale", "feed")),
    ("video-retail", 8_400_001, 400_000, [VIDEO, RETAIL], "creative-id", 2, FLIGHT_ALWAYS_ON,
     ("brand-a", "always-on", "homepage")),
    ("e7-meta-video", 8_800_001, 500_000, [EDP7, META, VIDEO], "campaign", 1, FLIGHT_SPRING,
     ("brand-a", "spring-launch", "feed")),
    ("e7-meta-retail", 9_300_001, 300_000, [EDP7, META, RETAIL], "ad_group", 1, FLIGHT_SUMMER,
     ("brand-b", "summer-sale", "instream")),
    ("e7-video-retail", 9_600_001, 300_000, [EDP7, VIDEO, RETAIL], "creative-id", 1,
     FLIGHT_ALWAYS_ON, ("brand-b", "always-on", "homepage")),
    ("meta-video-retail", 9_900_001, 400_000, [META, VIDEO, RETAIL], "campaign", 1,
     FLIGHT_SPRING, ("brand-a", "spring-launch", "feed")),
    ("all4", 10_300_001, 300_000, [EDP7, META, VIDEO, RETAIL], "ad_group", 3,
     FLIGHT_ALWAYS_ON, ("brand-a", "always-on", "instream")),
    # Sub-sigma noise probe for #4204. Sized well under sigma so noise-dominated
    # reports get flagged/FAILED. At 10x epsilon sigma is ~18k, so 8k is ~0.4 sigma.
    ("noise", 10_600_001, 8_000, [EDP7], "ad_group", 1, FLIGHT_SUMMER,
     ("brand-b", "summer-sale-promo", "feed")),
]

# Highest VID any segment reaches, rounded up to a whole stripe.
REACHED_VID_END = 10_610_000

TRANCHES = 6
# (frequency, share of the tranche's VIDs). Mean frequency 2.1, monotonic K+ curve.
FREQUENCY_MIX = [(1, 0.40), (2, 0.30), (3, 0.20), (5, 0.10)]

# Engagement values cycled per block so threshold IQFs select real subsets.
COMPLETED_FRACTIONS = [0.0, 0.25, 0.5, 0.75, 1.0]
VIEWABLE_FRACTIONS = [0.0, 0.5, 1.0]

OUT_DIR = pathlib.Path("src/main/proto/wfa/measurement/loadtest/dataprovider")

LICENSE = """# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

GENERATED_BY = (
    "# GENERATED FILE - DO NOT EDIT.\n"
    "# Regenerate with:\n"
    "#   python3 src/main/python/wfa/measurement/loadtest/qa2026/generate_specs.py\n"
)


def date_proto(d, indent):
    pad = " " * indent
    return f"{pad}year: {d.year}\n{pad}month: {d.month}\n{pad}day: {d.day}\n"


def generate_population_spec():
    """30 demographic tuples interleaved in 10k-VID stripes, plus a filler."""
    tuples = [
        (g, a, s) for g in GENDERS for a in AGE_GROUPS for s in US_STATES
    ]
    assert len(tuples) == 30, len(tuples)

    out = [LICENSE, GENERATED_BY]
    out.append(
        "\n# proto-file: wfa/measurement/api/v2alpha/population_spec.proto\n"
        "# proto-message: PopulationSpec\n"
        "\n"
        f"# QA 2026 synthetic population: {TOTAL_POPULATION:,} VIDs.\n"
        f"#\n"
        f"# VIDs 1-{REACHED_VID_END:,} are reachable and are striped in "
        f"{STRIPE_SIZE:,}-VID blocks\n"
        f"# cycling round-robin through the {len(tuples)} "
        f"(gender x age_group x us_state) tuples, so one\n"
        f"# full cycle ({len(tuples) * STRIPE_SIZE:,} VIDs) fits inside the smallest "
        f"Venn region and every\n"
        f"# region carries the full demographic mix. Demographics are therefore "
        f"orthogonal to\n"
        f"# the Venn structure, which comes only from EDP membership.\n"
        f"#\n"
        f"# The remainder up to {TOTAL_POPULATION:,} is a single unreached filler "
        f"subpopulation. It\n"
        f"# generates no impressions; it exists so populationSpec.size stresses "
        f"frequency-vector\n"
        f"# allocation (a dense ByteArray sized to the whole population, per filter "
        f"sink).\n"
    )

    stripe_count = REACHED_VID_END // STRIPE_SIZE
    for i in range(stripe_count):
        gender, age_group, us_state = tuples[i % len(tuples)]
        start = i * STRIPE_SIZE + 1
        end_inclusive = (i + 1) * STRIPE_SIZE
        out.append(
            f"\nsubpopulations {{\n"
            f"  vid_ranges {{\n"
            f"    start_vid: {start}\n"
            f"    end_vid_inclusive: {end_inclusive}\n"
            f"  }}\n"
            f"  attributes {{\n"
            f"    [{COMMON_TYPE_URL}] {{\n"
            f"      gender: {gender}\n"
            f"      age_group: {age_group}\n"
            f"      us_state: {us_state}\n"
            f"    }}\n"
            f"  }}\n"
            f"}}\n"
        )

    # Filler: never reached, holds populationSpec.size at TOTAL_POPULATION.
    gender, age_group, us_state = tuples[stripe_count % len(tuples)]
    out.append(
        f"\n# Unreached filler.\n"
        f"subpopulations {{\n"
        f"  vid_ranges {{\n"
        f"    start_vid: {REACHED_VID_END + 1}\n"
        f"    end_vid_inclusive: {TOTAL_POPULATION}\n"
        f"  }}\n"
        f"  attributes {{\n"
        f"    [{COMMON_TYPE_URL}] {{\n"
        f"      gender: {gender}\n"
        f"      age_group: {age_group}\n"
        f"      us_state: {us_state}\n"
        f"    }}\n"
        f"  }}\n"
        f"}}\n"
    )
    return "".join(out), stripe_count + 1


def generate_segment_spec(name, vid_start, vid_count, flight):
    """One SyntheticEventGroupSpec.

    Every vid_range_spec is exactly one population stripe. SyntheticDataGeneration
    resolves the demographic attributes of a range by finding the single
    subpopulation that wholly contains it, so a range spanning two stripes fails
    with "Sub-population not found". Segment boundaries are multiples of
    STRIPE_SIZE, so stripes tile each segment exactly.

    Stripes are assigned round-robin to (tranche, frequency, media): tranches stagger
    first exposure across the flight so cumulative reach grows week over week, the
    frequency mix gives a non-flat monotonic K+ curve, and alternating media means
    every segment emits both VIDEO and DISPLAY.
    """
    flight_start, flight_end = flight
    flight_days = (flight_end - flight_start).days

    stripes = []
    cursor = vid_start
    while cursor < vid_start + vid_count:
        end = min(cursor + STRIPE_SIZE, vid_start + vid_count)
        # Clamp to the enclosing stripe boundary so a range never straddles two.
        boundary = ((cursor - 1) // STRIPE_SIZE + 1) * STRIPE_SIZE + 1
        stripes.append((cursor, min(end, boundary)))
        cursor = stripes[-1][1]

    tranche_count = min(TRANCHES, len(stripes))
    # Expand the frequency mix into a repeating pattern of the right proportions.
    pattern = []
    for frequency, share in FREQUENCY_MIX:
        pattern.extend([frequency] * max(1, round(share * 10)))

    # (tranche, frequency) -> list of (start, end_exclusive, media)
    blocks = {}
    for i, (lo, hi) in enumerate(stripes):
        tranche = (i * tranche_count) // len(stripes)
        frequency = pattern[i % len(pattern)]
        media = "video" if i % 2 == 0 else "display"
        blocks.setdefault((tranche, frequency), []).append((lo, hi, media))

    out = [LICENSE, GENERATED_BY]
    out.append(
        "\n# proto-file: wfa/measurement/api/v2alpha/event_group_metadata/testing/"
        "simulator_synthetic_data_spec.proto\n"
        "# proto-message: SyntheticEventGroupSpec\n"
        "\n"
        f'description: "QA 2026 segment {name}: VIDs {vid_start:,}-'
        f'{vid_start + vid_count - 1:,} over '
        f'{flight_start.isoformat()}..{flight_end.isoformat()}"\n'
    )

    impressions = 0
    for tranche in range(tranche_count):
        start_date = flight_start + datetime.timedelta(
            days=(flight_days * tranche) // tranche_count
        )
        frequencies = sorted(f for (t, f) in blocks if t == tranche)
        if not frequencies:
            continue
        out.append(
            "\ndate_specs {\n"
            "  date_range {\n"
            "    start {\n"
            + date_proto(start_date, 6)
            + "    }\n"
            "    end_exclusive {\n"
            + date_proto(flight_end, 6)
            + "    }\n"
            "  }\n"
        )
        for frequency in frequencies:
            out.append(f"  frequency_specs {{\n    frequency: {frequency}\n")
            for n, (lo, hi, media) in enumerate(blocks[(tranche, frequency)]):
                out.append(
                    f"    vid_range_specs {{\n"
                    f"      vid_range {{\n"
                    f"        start: {lo}\n"
                    f"        end_exclusive: {hi}\n"
                    f"      }}\n"
                )
                if media == "video":
                    completed = COMPLETED_FRACTIONS[n % len(COMPLETED_FRACTIONS)]
                    viewable = VIEWABLE_FRACTIONS[n % len(VIEWABLE_FRACTIONS)]
                    out.append(
                        f"      non_population_field_values {{\n"
                        f'        key: "video.completed_fraction"\n'
                        f"        value {{\n"
                        f"          float_value: {completed}\n"
                        f"        }}\n"
                        f"      }}\n"
                        f"      non_population_field_values {{\n"
                        f'        key: "video.viewable_fraction"\n'
                        f"        value {{\n"
                        f"          float_value: {viewable}\n"
                        f"        }}\n"
                        f"      }}\n"
                    )
                else:
                    viewable = VIEWABLE_FRACTIONS[(n + 1) % len(VIEWABLE_FRACTIONS)]
                    out.append(
                        f"      non_population_field_values {{\n"
                        f'        key: "display.viewable_fraction"\n'
                        f"        value {{\n"
                        f"          float_value: {viewable}\n"
                        f"        }}\n"
                        f"      }}\n"
                    )
                out.append("    }\n")
                impressions += (hi - lo) * frequency
            out.append("  }\n")
        out.append("}\n")

    return "".join(out), impressions


def spec_filename(name):
    return f"qa2026_seg_{name.replace('-', '_')}.textproto"


def generate_config():
    """One ImpressionTestDataConfig entry per (segment, EDP) pair."""
    out = [LICENSE, GENERATED_BY]
    out.append(
        "\n# proto-file: src/main/proto/wfa/measurement/integration/k8s/testing/"
        "impression_test_data_config.proto\n"
        "# proto-message: wfa.measurement.integration.k8s.testing."
        "ImpressionTestDataConfig\n"
        "\n"
        "# QA 2026 dataset. Each segment is seeded on exactly the EDPs in its Venn\n"
        "# region, so selecting the whole menu realizes all 15 primitive regions.\n"
        "# Every event group carries an entity key: EventGroupSync filters its\n"
        "# existence check by entity type, so an event group without one is invisible\n"
        "# to that check and gets created again on every sync.\n"
        "\n"
        'population_spec_resource_path: "qa2026_population_spec.textproto"\n'
    )

    group_count = 0
    for (name, _vs, _vc, edps, entity_type, entity_count, _fl, meta) in SEGMENTS:
        brand, campaign, placement = meta
        for edp in edps:
            ref_id = f"qa2026-{name}-{edp}"
            out.append(
                f"\n# {name}: {' + '.join(edps)}\n"
                f"event_groups {{\n"
                f'  event_group_reference_id: "{ref_id}"\n'
                f'  edp_name: "{edp}"\n'
                f'  output_base_path: "edp/{edp}"\n'
                f'  output_key: "qa2026-{name}"\n'
            )
            for i in range(entity_count):
                suffix = "" if entity_count == 1 else f"-{i + 1}"
                out.append(
                    f"  entity_key_specs {{\n"
                    f'    entity_type: "{entity_type}"\n'
                    f'    entity_id: "qa2026-{name}-{edp}{suffix}"\n'
                    f'    data_spec_resource_path: "{spec_filename(name)}"\n'
                    f"  }}\n"
                )
            out.append(
                f"  entity_metadata {{\n"
                f"    fields {{\n"
                f'      key: "brand"\n'
                f'      value {{ string_value: "{brand}" }}\n'
                f"    }}\n"
                f"    fields {{\n"
                f'      key: "campaign_name"\n'
                f'      value {{ string_value: "{campaign}" }}\n'
                f"    }}\n"
                f"    fields {{\n"
                f'      key: "placement"\n'
                f'      value {{ string_value: "{placement}" }}\n'
                f"    }}\n"
                f"  }}\n"
                f"}}\n"
            )
            group_count += 1
    return "".join(out), group_count


def main():
    repo_root = pathlib.Path(__file__).resolve().parents[7]
    out_dir = repo_root / OUT_DIR
    if not out_dir.is_dir():
        raise SystemExit(f"output dir not found: {out_dir}")

    pop, subpop_count = generate_population_spec()
    (out_dir / "qa2026_population_spec.textproto").write_text(pop)

    per_edp = {}
    total_impressions = 0
    for (name, vid_start, vid_count, edps, _et, _ec, flight, _m) in SEGMENTS:
        spec, impressions = generate_segment_spec(name, vid_start, vid_count, flight)
        (out_dir / spec_filename(name)).write_text(spec)
        for edp in edps:
            per_edp.setdefault(edp, [0, 0])
            per_edp[edp][0] += vid_count
            per_edp[edp][1] += impressions
        total_impressions += impressions * len(edps)

    config, group_count = generate_config()
    (out_dir / "qa2026_impression_test_data_config.textproto").write_text(config)

    print(f"population spec : {subpop_count:,} subpopulations "
          f"({REACHED_VID_END:,} reached of {TOTAL_POPULATION:,})")
    print(f"segment specs   : {len(SEGMENTS)}")
    print(f"event groups    : {group_count}")
    print(f"impressions     : {total_impressions:,}\n")
    print(f"{'EDP':<18}{'reach':>12}{'impressions':>15}{'avg freq':>10}")
    for edp, (reach, impressions) in sorted(per_edp.items()):
        print(f"{edp:<18}{reach:>12,}{impressions:>15,}{impressions / reach:>10.2f}")


if __name__ == "__main__":
    main()
