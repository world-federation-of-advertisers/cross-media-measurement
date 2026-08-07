// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/labeler/labeler.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::IsOk;

constexpr int kEventCount = 200;

// Build a model: BranchNode with 50/50 chance between two
// RankedPopulationNode leaves — one DISJOINT, one FULL_POOL.
CompiledNode BuildTwoLeafModel() {
  CompiledNode root;
  google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "Root"
        branch_node {
          branches {
            node {
              name: "DisjointLeaf"
              ranked_population_node {
                pools { population_offset: 1000 total_population: 500 }
                random_seed: "disjoint-seed"
                ranked_size: 200
                unranked_mode: DISJOINT
              }
            }
            chance: 0.5
          }
          branches {
            node {
              name: "FullPoolLeaf"
              ranked_population_node {
                pools { population_offset: 5000 total_population: 500 }
                random_seed: "fullpool-seed"
                ranked_size: 200
                unranked_mode: FULL_POOL
              }
            }
            chance: 0.5
          }
          random_seed: "branch-seed"
        }
      )pb",
      &root);
  return root;
}

// Two-pass end-to-end test.
TEST(RankedLabelingIntegrationTest, TwoPassCollisionFree) {
  CompiledNode root = BuildTwoLeafModel();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  // === Pass 1: collect pool assignments ===
  // Track which events go to which pool.
  struct EventInfo {
    LabelerInput input;
    uint64_t pool_offset;
    uint64_t pool_size;
    uint64_t ranked_size;
  };
  std::vector<EventInfo> events;
  // Per-pool rank counter for simulated ranking.
  std::unordered_map<uint64_t, uint64_t> pool_rank_counter;

  for (int i = 0; i < kEventCount; ++i) {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test-pub");
    input.mutable_event_id()->set_id(std::to_string(i));

    LabelerOutput output;
    ASSERT_THAT(labeler->Label(input, output, LabelingMode::kPoolIdentity),
                IsOk());

    // Pass-1 should produce pool_assignments.
    ASSERT_EQ(output.pool_assignments_size(), 1)
        << "Event " << i << " should have exactly 1 pool assignment";
    EXPECT_EQ(output.people_size(), 0)
        << "Event " << i << " should have no people in pass-1";

    const auto& pa = output.pool_assignments(0);
    events.push_back(
        {input, pa.pool_offset(), pa.pool_size(), pa.ranked_size()});
    pool_rank_counter[pa.pool_offset()]++;  // count events per pool
  }

  // Verify events were split across both pools.
  EXPECT_GT(pool_rank_counter[1000], 0) << "No events routed to DISJOINT pool";
  EXPECT_GT(pool_rank_counter[5000], 0) << "No events routed to FULL_POOL pool";

  // === Simulate ranking: assign sequential ranks per pool ===
  std::unordered_map<uint64_t, uint64_t> pool_next_rank;
  for (auto& ev : events) {
    uint64_t rank = pool_next_rank[ev.pool_offset]++;
    // Add rank assignment to the input.
    auto* ra = ev.input.add_rank_assignments();
    ra->set_pool_offset(ev.pool_offset);
    ra->set_local_rank(rank);
  }

  // === Pass 2: assign VIDs ===
  std::unordered_map<uint64_t, std::unordered_set<uint64_t>> ranked_vids;
  int disjoint_unranked_count = 0;
  int fullpool_unranked_count = 0;

  for (const auto& ev : events) {
    LabelerOutput output;
    ASSERT_THAT(labeler->Label(ev.input, output), IsOk());

    ASSERT_EQ(output.people_size(), 1);
    EXPECT_TRUE(output.people(0).has_virtual_person_id());
    uint64_t vid = output.people(0).virtual_person_id();

    // Find the rank for this event.
    uint64_t local_rank = 0;
    for (const auto& ra : ev.input.rank_assignments()) {
      if (ra.pool_offset() == ev.pool_offset) {
        local_rank = ra.local_rank();
        break;
      }
    }

    if (local_rank < ev.ranked_size) {
      // RANKED event: VID must be in [pool_offset, pool_offset + ranked_size).
      EXPECT_GE(vid, ev.pool_offset)
          << "Ranked VID below pool_offset for pool " << ev.pool_offset;
      EXPECT_LT(vid, ev.pool_offset + ev.ranked_size)
          << "Ranked VID above ranked range for pool " << ev.pool_offset;
      ranked_vids[ev.pool_offset].insert(vid);
    } else {
      // UNRANKED event: check range depends on mode.
      if (ev.pool_offset == 1000) {
        // DISJOINT: VID in [pool_offset + ranked_size, pool_offset +
        // pool_size).
        EXPECT_GE(vid, ev.pool_offset + ev.ranked_size);
        EXPECT_LT(vid, ev.pool_offset + ev.pool_size);
        disjoint_unranked_count++;
      } else {
        // FULL_POOL: VID in [pool_offset, pool_offset + pool_size).
        EXPECT_GE(vid, ev.pool_offset);
        EXPECT_LT(vid, ev.pool_offset + ev.pool_size);
        fullpool_unranked_count++;
      }
    }
  }

  // Verify collision-free property: all ranked VIDs per pool are unique.
  for (const auto& [pool_offset, vids] : ranked_vids) {
    uint64_t ranked_size = (pool_offset == 1000) ? 200 : 200;
    // Number of ranked events = min(events_in_pool, ranked_size).
    uint64_t events_in_pool = pool_rank_counter[pool_offset];
    uint64_t expected_ranked = std::min(events_in_pool, ranked_size);
    EXPECT_EQ(vids.size(), expected_ranked)
        << "Collision detected in ranked VIDs for pool " << pool_offset
        << ": got " << vids.size() << " unique VIDs from " << expected_ranked
        << " ranked events";
  }
}

// Mixed model: one PopulationNode leaf, one RankedPopulationNode leaf.
TEST(RankedLabelingIntegrationTest, Pass1BranchRoutesToCorrectPools) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "Root"
        branch_node {
          branches {
            node {
              name: "PoolA"
              ranked_population_node {
                pools { population_offset: 1000 total_population: 500 }
                random_seed: "pool-a-seed"
                ranked_size: 200
                unranked_mode: DISJOINT
              }
            }
            chance: 0.5
          }
          branches {
            node {
              name: "PoolB"
              ranked_population_node {
                pools { population_offset: 5000 total_population: 500 }
                random_seed: "pool-b-seed"
                ranked_size: 300
                unranked_mode: FULL_POOL
              }
            }
            chance: 0.5
          }
          random_seed: "branch-seed"
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  std::unordered_set<uint64_t> pool_offsets;
  for (int i = 0; i < kEventCount; ++i) {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id(std::to_string(i));

    LabelerOutput output;
    ASSERT_THAT(labeler->Label(input, output, LabelingMode::kPoolIdentity),
                IsOk());
    ASSERT_EQ(output.pool_assignments_size(), 1);
    EXPECT_EQ(output.people_size(), 0);
    pool_offsets.insert(output.pool_assignments(0).pool_offset());
  }

  // Pool identity must vary with routing.
  EXPECT_GT(pool_offsets.count(1000), 0) << "No events routed to pool A";
  EXPECT_GT(pool_offsets.count(5000), 0) << "No events routed to pool B";
}

// Verify pass-2 with ranked_size=0 behaves like hash-based.
TEST(RankedLabelingIntegrationTest, RankedSizeZeroFullHashBased) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "HashOnlyNode"
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "hash-only-seed"
          ranked_size: 0
          unranked_mode: FULL_POOL
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  for (int i = 0; i < 50; ++i) {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id(std::to_string(i));

    LabelerOutput output;
    ASSERT_THAT(labeler->Label(input, output), IsOk());
    ASSERT_EQ(output.people_size(), 1);
    uint64_t vid = output.people(0).virtual_person_id();
    EXPECT_GE(vid, 100);
    EXPECT_LT(vid, 1100);
  }
}

// Mixed model: one RankedPopulationNode leaf and one (vanilla) PopulationNode
// leaf under a BranchNode. In pass-1, events routing to the ranked leaf emit a
// PoolAssignment; events routing to the PopulationNode leaf emit nothing. No
// VIDs are assigned in either case.
TEST(RankedLabelingIntegrationTest,
     Pass1MixedModelEmitsAssignmentsOnlyForRankedLeaf) {
  CompiledNode root;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "Root"
        branch_node {
          branches {
            node {
              name: "RankedLeaf"
              ranked_population_node {
                pools { population_offset: 1000 total_population: 500 }
                random_seed: "ranked-seed"
                ranked_size: 200
                unranked_mode: DISJOINT
              }
            }
            chance: 0.5
          }
          branches {
            node {
              name: "VanillaLeaf"
              population_node {
                pools { population_offset: 10 total_population: 100 }
                random_seed: "vanilla-seed"
              }
            }
            chance: 0.5
          }
          random_seed: "branch-seed"
        }
      )pb",
      &root));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> labeler, Labeler::Build(root));

  int ranked_count = 0;
  int vanilla_count = 0;
  for (int i = 0; i < kEventCount; ++i) {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id(std::to_string(i));

    LabelerOutput output;
    ASSERT_THAT(labeler->Label(input, output, LabelingMode::kPoolIdentity),
                IsOk());
    EXPECT_EQ(output.people_size(), 0);
    if (output.pool_assignments_size() == 1) {
      EXPECT_EQ(output.pool_assignments(0).pool_offset(), 1000);
      ++ranked_count;
    } else {
      EXPECT_EQ(output.pool_assignments_size(), 0);
      ++vanilla_count;
    }
  }

  EXPECT_GT(ranked_count, 0) << "No events routed to the ranked leaf";
  EXPECT_GT(vanilla_count, 0) << "No events routed to the vanilla leaf";
  EXPECT_EQ(ranked_count + vanilla_count, kEventCount);
}

// Cross-language golden vectors. These exact VIDs and PoolAssignment values
// must be reproduced by the Kotlin implementation for the same config and
// inputs; a matching Kotlin golden test (separate PR) pins the other side. The
// Feistel golden vectors in
// PR #81 only cover the ranked permutation; these additionally pin the unranked
// path (FarmHash64(random_seed + acting_fingerprint) -> JumpConsistentHash) and
// the pass-1 PoolAssignment, so a divergent seed construction or hash binding
// in either language fails loudly instead of silently assigning different VIDs.
TEST(RankedLabelingIntegrationTest, CrossLanguageGoldenVectors) {
  CompiledNode disjoint;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "GoldenDisjoint"
        ranked_population_node {
          pools { population_offset: 1000 total_population: 500 }
          random_seed: "golden-seed"
          ranked_size: 200
          unranked_mode: DISJOINT
        }
      )pb",
      &disjoint));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> disjoint_labeler,
                       Labeler::Build(disjoint));

  // (1) Full ranked path: rank 5 -> pool_offset + Feistel(5, ranked_size,
  // seed).
  {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id("golden-ranked");
    RankAssignment* ra = input.add_rank_assignments();
    ra->set_pool_offset(1000);
    ra->set_local_rank(5);
    LabelerOutput output;
    ASSERT_THAT(disjoint_labeler->Label(input, output), IsOk());
    ASSERT_EQ(output.people_size(), 1);
    EXPECT_EQ(output.people(0).virtual_person_id(), 1141);
  }

  // (2) Unranked DISJOINT path: no rank -> [pool_offset+ranked_size,
  // +pool_size).
  {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id("golden-unranked");
    LabelerOutput output;
    ASSERT_THAT(disjoint_labeler->Label(input, output), IsOk());
    ASSERT_EQ(output.people_size(), 1);
    EXPECT_EQ(output.people(0).virtual_person_id(), 1354);
  }

  // (4) Pass-1 mode: emits PoolAssignment(pool_offset, pool_size, ranked_size).
  {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id("golden-pass1");
    LabelerOutput output;
    ASSERT_THAT(
        disjoint_labeler->Label(input, output, LabelingMode::kPoolIdentity),
        IsOk());
    ASSERT_EQ(output.pool_assignments_size(), 1);
    EXPECT_EQ(output.pool_assignments(0).pool_offset(), 1000);
    EXPECT_EQ(output.pool_assignments(0).pool_size(), 500);
    EXPECT_EQ(output.pool_assignments(0).ranked_size(), 200);
    EXPECT_EQ(output.people_size(), 0);
  }

  // (3) Unranked FULL_POOL path: no rank -> [pool_offset,
  // pool_offset+pool_size).
  CompiledNode full_pool;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "GoldenFullPool"
        ranked_population_node {
          pools { population_offset: 5000 total_population: 500 }
          random_seed: "golden-seed"
          ranked_size: 200
          unranked_mode: FULL_POOL
        }
      )pb",
      &full_pool));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Labeler> full_pool_labeler,
                       Labeler::Build(full_pool));
  {
    LabelerInput input;
    input.mutable_event_id()->set_publisher("test");
    input.mutable_event_id()->set_id("golden-fullpool");
    LabelerOutput output;
    ASSERT_THAT(full_pool_labeler->Label(input, output), IsOk());
    ASSERT_EQ(output.people_size(), 1);
    EXPECT_EQ(output.people(0).virtual_person_id(), 5472);
  }
}

}  // namespace
}  // namespace wfa_virtual_people
