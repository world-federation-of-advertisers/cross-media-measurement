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

absl::StatusOr<std::string> CompleteExecutionPhaseOne(
    const std::string& serialized_request) {
  CompleteExecutionPhaseOneRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseOneResponse result,
                   CompleteExecutionPhaseOne(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteExecutionPhaseOneAtAggregator(
    const std::string& serialized_request) {
  CompleteExecutionPhaseOneAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseOneAtAggregatorResponse result,
                   CompleteExecutionPhaseOneAtAggregator(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteExecutionPhaseTwo(
    const std::string& serialized_request) {
  CompleteExecutionPhaseTwoRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseTwoResponse result,
                   CompleteExecutionPhaseTwo(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteExecutionPhaseTwoAtAggregator(
    const std::string& serialized_request) {
  CompleteExecutionPhaseTwoAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseTwoAtAggregatorResponse result,
                   CompleteExecutionPhaseTwoAtAggregator(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteExecutionPhaseThree(
    const std::string& serialized_request) {
  CompleteExecutionPhaseThreeRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseThreeResponse result,
                   CompleteExecutionPhaseThree(request_proto));
  return result.SerializeAsString();
};

absl::StatusOr<std::string> CompleteExecutionPhaseThreeAtAggregator(
    const std::string& serialized_request) {
  CompleteExecutionPhaseThreeAtAggregatorRequest request_proto;
  RETURN_IF_ERROR(ParseRequestFromString(request_proto, serialized_request));
  ASSIGN_OR_RETURN(CompleteExecutionPhaseThreeAtAggregatorResponse result,
                   CompleteExecutionPhaseThreeAtAggregator(request_proto));
  return result.SerializeAsString();
};

}  // namespace crypto
}  // namespace common
}  // namespace measurement
}  // namespace wfa