// Copyright 2020 The Measurement System Authors
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

#include "absl/status/statusor.h"
#include "util/status_macros.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_methods.pb.h"
#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h"
#include "wfa/measurement/common/macros.h"

namespace wfa {
namespace measurement {
namespace common {
namespace crypto {

absl::StatusOr<std::string> CompleteSetupPhase(
    const std::string& serialized_request) {
  CompleteSetupPhaseRequest request_proto;

  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteSetupPhaseResponse result,
                   CompleteSetupPhase(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteReachEstimationPhase(
    const std::string& serialized_request) {
  CompleteReachEstimationPhaseRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteReachEstimationPhaseResponse result,
                   CompleteReachEstimationPhase(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteReachEstimationPhaseAtAggregator(
    const std::string& serialized_request) {
  CompleteReachEstimationPhaseAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteReachEstimationPhaseAtAggregatorResponse result,
                   CompleteReachEstimationPhaseAtAggregator(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteFilteringPhase(
    const std::string& serialized_request) {
  CompleteFilteringPhaseRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteFilteringPhaseResponse result,
                   CompleteFilteringPhase(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteFilteringPhaseAtAggregator(
    const std::string& serialized_request) {
  CompleteFilteringPhaseAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteFilteringPhaseAtAggregatorResponse result,
                   CompleteFilteringPhaseAtAggregator(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteFrequencyEstimationPhase(
    const std::string& serialized_request) {
  CompleteFrequencyEstimationPhaseRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteFrequencyEstimationPhaseResponse result,
                   CompleteFrequencyEstimationPhase(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteFrequencyEstimationPhaseAtAggregator(
    const std::string& serialized_request) {
  CompleteFrequencyEstimationPhaseAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteFrequencyEstimationPhaseAtAggregatorResponse result,
                   CompleteFrequencyEstimationPhaseAtAggregator(request_proto));
  return result.SerializeAsString();
};

}  // namespace crypto
}  // namespace common
}  // namespace measurement
}  // namespace wfa