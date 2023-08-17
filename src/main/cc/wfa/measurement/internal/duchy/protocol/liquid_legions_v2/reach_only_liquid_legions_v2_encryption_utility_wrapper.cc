// Copyright 2023 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/reach_only_liquid_legions_v2_encryption_utility_wrapper.h"

#include <string>

#include "absl/status/statusor.h"
#include "common_cpp/jni/jni_wrap.h"
#include "common_cpp/macros/macros.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/reach_only_liquid_legions_v2_encryption_utility.h"
#include "wfa/measurement/internal/duchy/protocol/reach_only_liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

absl::StatusOr<std::string> CompleteReachOnlyInitializationPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteReachOnlyInitializationPhaseRequest,
                 CompleteReachOnlyInitializationPhaseResponse>(
      serialized_request, CompleteReachOnlyInitializationPhase);
}

absl::StatusOr<std::string> CompleteReachOnlySetupPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteReachOnlySetupPhaseRequest,
                 CompleteReachOnlySetupPhaseResponse>(
      serialized_request, CompleteReachOnlySetupPhase);
}

absl::StatusOr<std::string> CompleteReachOnlySetupPhaseAtAggregator(
    const std::string& serialized_request) {
  return JniWrap<CompleteReachOnlySetupPhaseRequest,
                 CompleteReachOnlySetupPhaseResponse>(
      serialized_request, CompleteReachOnlySetupPhaseAtAggregator);
}

absl::StatusOr<std::string> CompleteReachOnlyExecutionPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteReachOnlyExecutionPhaseRequest,
                 CompleteReachOnlyExecutionPhaseResponse>(
      serialized_request, CompleteReachOnlyExecutionPhase);
}

absl::StatusOr<std::string> CompleteReachOnlyExecutionPhaseAtAggregator(
    const std::string& serialized_request) {
  return JniWrap<CompleteReachOnlyExecutionPhaseAtAggregatorRequest,
                 CompleteReachOnlyExecutionPhaseAtAggregatorResponse>(
      serialized_request, CompleteReachOnlyExecutionPhaseAtAggregator);
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
