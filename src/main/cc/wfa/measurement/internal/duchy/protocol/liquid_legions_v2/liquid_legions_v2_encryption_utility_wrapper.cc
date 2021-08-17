// Copyright 2020 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/liquid_legions_v2_encryption_utility_wrapper.h"

#include <string>

#include "absl/status/statusor.h"
#include "common_cpp/jni/jni_wrap.h"
#include "common_cpp/macros/macros.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/liquid_legions_v2_encryption_utility.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

absl::StatusOr<std::string> CompleteInitializationPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteInitializationPhaseRequest,
                 CompleteInitializationPhaseResponse>(
      serialized_request, CompleteInitializationPhase);
}

absl::StatusOr<std::string> CompleteSetupPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteSetupPhaseRequest, CompleteSetupPhaseResponse>(
      serialized_request, CompleteSetupPhase);
}

absl::StatusOr<std::string> CompleteExecutionPhaseOne(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseOneRequest,
                 CompleteExecutionPhaseOneResponse>(serialized_request,
                                                    CompleteExecutionPhaseOne);
}

absl::StatusOr<std::string> CompleteExecutionPhaseOneAtAggregator(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseOneAtAggregatorRequest,
                 CompleteExecutionPhaseOneAtAggregatorResponse>(
      serialized_request, CompleteExecutionPhaseOneAtAggregator);
}

absl::StatusOr<std::string> CompleteExecutionPhaseTwo(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseTwoRequest,
                 CompleteExecutionPhaseTwoResponse>(serialized_request,
                                                    CompleteExecutionPhaseTwo);
}

absl::StatusOr<std::string> CompleteExecutionPhaseTwoAtAggregator(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseTwoAtAggregatorRequest,
                 CompleteExecutionPhaseTwoAtAggregatorResponse>(
      serialized_request, CompleteExecutionPhaseTwoAtAggregator);
}

absl::StatusOr<std::string> CompleteExecutionPhaseThree(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseThreeRequest,
                 CompleteExecutionPhaseThreeResponse>(
      serialized_request, CompleteExecutionPhaseThree);
}

absl::StatusOr<std::string> CompleteExecutionPhaseThreeAtAggregator(
    const std::string& serialized_request) {
  return JniWrap<CompleteExecutionPhaseThreeAtAggregatorRequest,
                 CompleteExecutionPhaseThreeAtAggregatorResponse>(
      serialized_request, CompleteExecutionPhaseThreeAtAggregator);
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
