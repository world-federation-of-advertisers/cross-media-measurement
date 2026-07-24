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

#include "wfa/virtual_people/core/model/ranked_population_node_impl.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "common_cpp/macros/macros.h"
#include "src/farmhash.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"
#include "wfa/virtual_people/core/model/utils/consistent_hash.h"
#include "wfa/virtual_people/core/model/utils/feistel.h"
#include "wfa/virtual_people/core/model/utils/population_node_helper.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<RankedPopulationNodeImpl>>
RankedPopulationNodeImpl::Build(const CompiledNode& node_config) {
  if (!node_config.has_ranked_population_node()) {
    return absl::InvalidArgumentError("This is not a ranked population node.");
  }
  const auto& config = node_config.ranked_population_node();

  if (config.pools_size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("RankedPopulationNode requires exactly one pool, got ",
                     config.pools_size(), "."));
  }

  uint64_t pool_offset = config.pools(0).population_offset();
  uint64_t pool_size = config.pools(0).total_population();

  if (pool_size == 0) {
    return absl::InvalidArgumentError(
        "RankedPopulationNode total pool size must be > 0.");
  }

  // JumpConsistentHash takes an int32_t bucket count, so the pool must fit in
  // INT32_MAX. The design worst case (~1.5B) fits; guard larger pools so a
  // future model fails loudly instead of silently wrapping to a wrong VID.
  if (pool_size > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
    return absl::InvalidArgumentError(absl::StrCat(
        "pool_size (", pool_size, ") exceeds the maximum supported size (",
        std::numeric_limits<int32_t>::max(), ")."));
  }

  if (config.ranked_size() > pool_size) {
    return absl::InvalidArgumentError(
        absl::StrCat("ranked_size (", config.ranked_size(),
                     ") must not exceed pool_size (", pool_size, ")."));
  }

  return absl::make_unique<RankedPopulationNodeImpl>(
      node_config, config.random_seed(), config.ranked_size(),
      config.unranked_mode(), pool_offset, pool_size);
}

RankedPopulationNodeImpl::RankedPopulationNodeImpl(
    const CompiledNode& node_config, std::string random_seed,
    uint64_t ranked_size, RankedPopulationNode::UnrankedMode unranked_mode,
    uint64_t pool_offset, uint64_t pool_size)
    : ModelNode(node_config),
      random_seed_(std::move(random_seed)),
      ranked_size_(ranked_size),
      unranked_mode_(unranked_mode),
      pool_offset_(pool_offset),
      pool_size_(pool_size) {}

absl::Status RankedPopulationNodeImpl::Apply(LabelerEvent& event) const {
  // Pass-1 mode: emit pool identity and return without assigning a VID.
  if (event.pool_identity_mode()) {
    PoolAssignment* pa = event.add_pool_assignments();
    pa->set_pool_offset(pool_offset_);
    pa->set_pool_size(pool_size_);
    pa->set_ranked_size(ranked_size_);
    return absl::OkStatus();
  }

  if (event.virtual_person_activities_size() > 0) {
    return absl::InvalidArgumentError(
        "virtual_person_activities should only be created in leaf nodes.");
  }

  VirtualPersonActivity* activity = event.add_virtual_person_activities();
  uint64_t virtual_person_id;

  // Look up pre-computed rank from LabelerInput.rank_assignments. The caller
  // (memoized Phase-2 sink) attaches a RankAssignment for every subpool the
  // fingerprint is currently ranked in across this (DataProvider, ModelLine);
  // this leaf scans the list for the entry matching its own pool_offset. If
  // none matches, fall back to the hash path — same behavior as if no rank
  // assignments were provided at all. The primary case for this fallback is
  // overflow: when this subpool reached ranked_size in Phase 1 and this
  // fingerprint was one of the unranked surplus, the subpool has no rank for
  // it (per the design's overflow-fps-fall-back-to-unranked-path contract).
  // Also defensively covers operator scenarios like heal-rank-index after
  // partial data loss or a model-version mismatch between Phase 0 and Phase 2.
  bool had_rank_assignments = event.has_labeler_input() &&
                              event.labeler_input().rank_assignments_size() > 0;
  bool has_rank = false;
  uint64_t local_rank = 0;
  if (event.has_labeler_input()) {
    for (const auto& ra : event.labeler_input().rank_assignments()) {
      if (ra.pool_offset() == pool_offset_) {
        local_rank = ra.local_rank();
        has_rank = true;
        break;
      }
    }
  }

  // Memoized-rank fallback signal: stamped iff the caller attempted memoization
  // (rank_assignments non-empty) but we end up on the hash path. The consumer
  // aggregates this into an OpenTelemetry counter to alarm on rising
  // memoized-fallback rates per (data provider, model line, pool offset).
  if (had_rank_assignments && !(has_rank && local_rank < ranked_size_)) {
    activity->set_memoized_rank_fallback(true);
  }

  if (has_rank && local_rank < ranked_size_) {
    // RANKED path: Feistel bijection — zero collisions.
    virtual_person_id =
        pool_offset_ + FeistelPermute(local_rank, ranked_size_, random_seed_);
  } else {
    // UNRANKED path: hash-based (mode-dependent scope).
    std::string seed_str =
        absl::StrCat(random_seed_, event.acting_fingerprint());
    uint64_t seed = util::Fingerprint64(seed_str.data(), seed_str.size());

    if (unranked_mode_ == RankedPopulationNode::DISJOINT) {
      uint64_t unranked_size = pool_size_ - ranked_size_;
      if (unranked_size == 0) {
        return absl::InvalidArgumentError(
            "DISJOINT mode with ranked_size == pool_size leaves no unranked "
            "space.");
      }
      virtual_person_id =
          pool_offset_ + ranked_size_ +
          JumpConsistentHash(seed, static_cast<int32_t>(unranked_size));
    } else if (unranked_mode_ == RankedPopulationNode::FULL_POOL) {
      // FULL_POOL: hash into the entire pool.
      virtual_person_id =
          pool_offset_ +
          JumpConsistentHash(seed, static_cast<int32_t>(pool_size_));
    } else {
      return absl::InvalidArgumentError(
          "UnrankedMode must be DISJOINT or FULL_POOL.");
    }
  }

  activity->set_virtual_person_id(virtual_person_id);

  // Collapse quantum labels from the event (same as PopulationNodeImpl).
  if (event.has_quantum_labels()) {
    std::string seed_suffix = std::to_string(virtual_person_id);
    for (const QuantumLabel& quantum_label :
         event.quantum_labels().quantum_labels()) {
      RETURN_IF_ERROR(CollapseQuantumLabel(quantum_label, seed_suffix,
                                           *activity->mutable_label()));
    }
  }
  // Merge classic label from the event.
  if (event.has_label()) {
    activity->mutable_label()->MergeFrom(event.label());
  }

  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
