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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_WRAPPER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_WRAPPER_H_

#include <string>

#include "absl/status/statusor.h"

// Wrapper methods used to generate the swig/JNI Java classes.
// The only functionality of these methods are converting between proto messages
// and their corresponding serialized strings, and then calling into the
// liquid_legions_v2_encryption_utility methods.
namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

absl::StatusOr<std::string> CompleteInitializationPhase(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteSetupPhase(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseOne(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseOneAtAggregator(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseTwo(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseTwoAtAggregator(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseThree(
    const std::string& serialized_request);

absl::StatusOr<std::string> CompleteExecutionPhaseThreeAtAggregator(
    const std::string& serialized_request);

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_LIQUID_LEGIONS_V2_LIQUID_LEGIONS_V2_ENCRYPTION_UTILITY_WRAPPER_H_
