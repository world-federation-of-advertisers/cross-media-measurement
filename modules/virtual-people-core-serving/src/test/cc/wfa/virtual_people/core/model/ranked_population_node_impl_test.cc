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

#include <cstdint>
#include <unordered_set>

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {
namespace {

using ::testing::Ge;
using ::testing::Lt;
using ::wfa::IsOk;
using ::wfa::StatusIs;

TEST(RankedPopulationNodeImplTest, RankedPathAssignsVidInRankedRange) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent event;
  event.set_acting_fingerprint(42);
  // Set a rank assignment that matches the pool offset.
  auto* ra = event.mutable_labeler_input()->add_rank_assignments();
  ra->set_pool_offset(100);
  ra->set_local_rank(7);

  EXPECT_THAT(node->Apply(event), IsOk());
  uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
  // Ranked VIDs must be in [pool_offset, pool_offset + ranked_size).
  EXPECT_GE(vid, 100);
  EXPECT_LT(vid, 600);
}

TEST(RankedPopulationNodeImplTest, UnrankedDisjointAssignsVidInUnrankedRange) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // No rank assignment — falls through to unranked path.
  LabelerEvent event;
  event.set_acting_fingerprint(42);

  EXPECT_THAT(node->Apply(event), IsOk());
  uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
  // DISJOINT: VIDs in [pool_offset + ranked_size, pool_offset + pool_size).
  EXPECT_GE(vid, 600);
  EXPECT_LT(vid, 1100);
}

TEST(RankedPopulationNodeImplTest, UnrankedFullPoolAssignsVidInFullRange) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: FULL_POOL
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent event;
  event.set_acting_fingerprint(42);

  EXPECT_THAT(node->Apply(event), IsOk());
  uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
  // FULL_POOL: VIDs in [pool_offset, pool_offset + pool_size).
  EXPECT_GE(vid, 100);
  EXPECT_LT(vid, 1100);
}

TEST(RankedPopulationNodeImplTest, RankedSizeZeroBehavesLikeHashBased) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 0
          unranked_mode: FULL_POOL
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent event;
  event.set_acting_fingerprint(42);

  EXPECT_THAT(node->Apply(event), IsOk());
  uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
  EXPECT_GE(vid, 100);
  EXPECT_LT(vid, 1100);
}

TEST(RankedPopulationNodeImplTest, CollisionFreeRankedEvents) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 0 total_population: 200 }
          random_seed: "collision-test"
          ranked_size: 100
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  std::unordered_set<uint64_t> vids;
  for (uint64_t rank = 0; rank < 100; ++rank) {
    LabelerEvent event;
    event.set_acting_fingerprint(rank);
    auto* ra = event.mutable_labeler_input()->add_rank_assignments();
    ra->set_pool_offset(0);
    ra->set_local_rank(rank);

    EXPECT_THAT(node->Apply(event), IsOk());
    uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
    EXPECT_GE(vid, 0);
    EXPECT_LT(vid, 100);
    vids.insert(vid);
  }
  // All 100 ranked events must produce 100 distinct VIDs.
  EXPECT_EQ(vids.size(), 100);
}

TEST(RankedPopulationNodeImplTest, Determinism) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "determinism-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node1,
                       ModelNode::Build(config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node2,
                       ModelNode::Build(config));

  for (int fp = 0; fp < 100; ++fp) {
    LabelerEvent event1;
    event1.set_acting_fingerprint(fp);
    LabelerEvent event2;
    event2.set_acting_fingerprint(fp);

    EXPECT_THAT(node1->Apply(event1), IsOk());
    EXPECT_THAT(node2->Apply(event2), IsOk());
    EXPECT_EQ(event1.virtual_person_activities(0).virtual_person_id(),
              event2.virtual_person_activities(0).virtual_person_id());
  }
}

TEST(RankedPopulationNodeImplTest, ExistingVirtualPersonError) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent event;
  event.add_virtual_person_activities()->set_virtual_person_id(42);
  event.set_acting_fingerprint(42);

  EXPECT_THAT(node->Apply(event),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(RankedPopulationNodeImplTest, InvalidEmptyPools) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          random_seed: "test-seed"
          ranked_size: 0
          unranked_mode: FULL_POOL
        }
      )pb",
      &config));

  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(RankedPopulationNodeImplTest, InvalidRankedSizeExceedsPoolSize) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 50 }
          random_seed: "test-seed"
          ranked_size: 100
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(RankedPopulationNodeImplTest, ApplyWithClassicLabel) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "test-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  LabelerEvent event;
  event.set_acting_fingerprint(42);
  event.mutable_label()->mutable_demo()->set_gender(GENDER_FEMALE);

  EXPECT_THAT(node->Apply(event), IsOk());
  EXPECT_TRUE(event.virtual_person_activities(0).has_label());
  EXPECT_EQ(event.virtual_person_activities(0).label().demo().gender(),
            GENDER_FEMALE);
}

TEST(RankedPopulationNodeImplTest, BoundaryRankEqualsRankedSizeFallsBack) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 1000 }
          random_seed: "boundary-seed"
          ranked_size: 500
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // local_rank=500 == ranked_size=500 — boundary, falls back to unranked.
  LabelerEvent event;
  event.set_acting_fingerprint(42);
  auto* ra = event.mutable_labeler_input()->add_rank_assignments();
  ra->set_pool_offset(100);
  ra->set_local_rank(500);

  EXPECT_THAT(node->Apply(event), IsOk());
  const auto& activity = event.virtual_person_activities(0);
  uint64_t vid = activity.virtual_person_id();
  // DISJOINT unranked range: [pool_offset + ranked_size, pool_offset +
  // pool_size).
  EXPECT_GE(vid, 600);
  EXPECT_LT(vid, 1100);
  // Caller attempted memoization (rank_assignments non-empty) but we fell back
  // to hash because local_rank >= ranked_size — surface the signal.
  EXPECT_TRUE(activity.memoized_rank_fallback());
}

TEST(RankedPopulationNodeImplTest, MultipleRankAssignmentsResolvesCorrectPool) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 500 total_population: 1000 }
          random_seed: "multi-rank-seed"
          ranked_size: 400
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // Event carries rank assignments for multiple pools; only 500 matches.
  LabelerEvent event;
  event.set_acting_fingerprint(42);
  auto* labeler_input = event.mutable_labeler_input();
  auto* ra1 = labeler_input->add_rank_assignments();
  ra1->set_pool_offset(100);
  ra1->set_local_rank(5);
  auto* ra2 = labeler_input->add_rank_assignments();
  ra2->set_pool_offset(500);
  ra2->set_local_rank(10);
  auto* ra3 = labeler_input->add_rank_assignments();
  ra3->set_pool_offset(9000);
  ra3->set_local_rank(99);

  EXPECT_THAT(node->Apply(event), IsOk());
  uint64_t vid = event.virtual_person_activities(0).virtual_person_id();
  // Uses pool_offset=500, local_rank=10 → ranked range [500, 900).
  EXPECT_GE(vid, 500);
  EXPECT_LT(vid, 900);
}

// When this leaf's pool_offset matches no entry in rank_assignments, the leaf
// must fall back to the hash path instead of failing the job. The dominant
// case is Phase-1 overflow: when this subpool reached ranked_size and this
// fingerprint was one of the unranked surplus, the subpool has no rank for it
// (per design § Retention Rule, overflow-fps-fall-back-to-unranked-path).
TEST(RankedPopulationNodeImplTest, NoMatchingRankAssignmentFallsBackToHash) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 500 }
          random_seed: "overflow-fallback-seed"
          ranked_size: 200
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // Rank assignments only carry entries for OTHER pools (9000, 99999) — none
  // for this leaf's pool_offset=100. This mirrors the multi-subpool case where
  // the fingerprint holds ranks in other subpools but not this one.
  LabelerEvent event;
  event.set_acting_fingerprint(42);
  auto* labeler_input = event.mutable_labeler_input();
  auto* ra1 = labeler_input->add_rank_assignments();
  ra1->set_pool_offset(9000);
  ra1->set_local_rank(5);
  auto* ra2 = labeler_input->add_rank_assignments();
  ra2->set_pool_offset(99999);
  ra2->set_local_rank(17);

  EXPECT_THAT(node->Apply(event), IsOk());
  const auto& activity = event.virtual_person_activities(0);
  uint64_t vid = activity.virtual_person_id();
  // Must land in the unranked sub-range [pool_offset + ranked_size,
  // pool_offset + pool_size) = [300, 600) under DISJOINT mode.
  EXPECT_GE(vid, 300);
  EXPECT_LT(vid, 600);
  // The whole point of this path is to surface that the leaf had to
  // hash-fall-back.
  EXPECT_TRUE(activity.memoized_rank_fallback());
}

// The non-matching-rank_assignments path must produce the same VID as the
// empty-rank_assignments path — both represent the same semantic state
// (no rank for this fingerprint in this subpool, hash-fall-back), so same
// input must yield the same VID. Locks in that the fix didn't accidentally
// introduce a different hash seed for the two no-rank cases.
TEST(RankedPopulationNodeImplTest,
     NoMatchingRankAssignmentMatchesEmptyAssignmentsVid) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 500 }
          random_seed: "overflow-fallback-seed"
          ranked_size: 200
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> node,
                       ModelNode::Build(config));

  // Event A: no rank_assignments at all.
  LabelerEvent event_a;
  event_a.set_acting_fingerprint(42);
  event_a.mutable_labeler_input();
  EXPECT_THAT(node->Apply(event_a), IsOk());
  const auto& activity_a = event_a.virtual_person_activities(0);
  uint64_t vid_a = activity_a.virtual_person_id();
  // No rank_assignments at all => caller did not attempt memoization, so the
  // fallback signal MUST NOT fire.
  EXPECT_FALSE(activity_a.memoized_rank_fallback());

  // Event B: same fingerprint, rank_assignments only for other pools.
  LabelerEvent event_b;
  event_b.set_acting_fingerprint(42);
  auto* ra = event_b.mutable_labeler_input()->add_rank_assignments();
  ra->set_pool_offset(9000);
  ra->set_local_rank(5);
  EXPECT_THAT(node->Apply(event_b), IsOk());
  const auto& activity_b = event_b.virtual_person_activities(0);
  uint64_t vid_b = activity_b.virtual_person_id();
  // Non-matching assignments => caller did attempt memoization but the leaf
  // hash-fell-back, so the signal MUST fire.
  EXPECT_TRUE(activity_b.memoized_rank_fallback());

  // Both must produce the same VID — non-matching assignments behave exactly
  // like empty assignments.
  EXPECT_EQ(vid_a, vid_b);
}

TEST(RankedPopulationNodeImplTest, InvalidMultiplePools) {
  CompiledNode config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "TestRankedNode"
        index: 1
        ranked_population_node {
          pools { population_offset: 100 total_population: 500 }
          pools { population_offset: 600 total_population: 500 }
          random_seed: "test-seed"
          ranked_size: 200
          unranked_mode: DISJOINT
        }
      )pb",
      &config));

  EXPECT_THAT(ModelNode::Build(config).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(RankedPopulationNodeImplTest, RankedSizeZeroMatchesPopulationNode) {
  // Backward-compatibility guarantee: a RankedPopulationNode with ranked_size=0
  // and FULL_POOL must assign exactly the same VIDs as a PopulationNode with
  // the same pool and seed. Deploying a memoized model with ranked_size=0 must
  // not change existing VID assignments.
  CompiledNode ranked_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "RankedNode"
        ranked_population_node {
          pools { population_offset: 100 total_population: 500 }
          random_seed: "same-seed"
          ranked_size: 0
          unranked_mode: FULL_POOL
        }
      )pb",
      &ranked_config));
  CompiledNode pop_config;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        name: "PopNode"
        population_node {
          pools { population_offset: 100 total_population: 500 }
          random_seed: "same-seed"
        }
      )pb",
      &pop_config));

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> ranked_node,
                       ModelNode::Build(ranked_config));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ModelNode> pop_node,
                       ModelNode::Build(pop_config));

  for (int fingerprint = 0; fingerprint < 100; ++fingerprint) {
    LabelerEvent ranked_event;
    ranked_event.set_acting_fingerprint(fingerprint);
    ASSERT_THAT(ranked_node->Apply(ranked_event), IsOk());
    ASSERT_EQ(ranked_event.virtual_person_activities_size(), 1);

    LabelerEvent pop_event;
    pop_event.set_acting_fingerprint(fingerprint);
    ASSERT_THAT(pop_node->Apply(pop_event), IsOk());
    ASSERT_EQ(pop_event.virtual_person_activities_size(), 1);

    EXPECT_EQ(ranked_event.virtual_person_activities(0).virtual_person_id(),
              pop_event.virtual_person_activities(0).virtual_person_id())
        << "VID mismatch for fingerprint " << fingerprint;
  }
}

}  // namespace
}  // namespace wfa_virtual_people
