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

#include "wfa/virtual_people/core/model/geometric_shredder_impl.h"

#include <cmath>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "common_cpp/macros/macros.h"
#include "google/protobuf/descriptor.h"
#include "src/farmhash.h"
#include "wfa/virtual_people/common/field_filter/utils/field_util.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/utils/hash.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<GeometricShredderImpl>>
GeometricShredderImpl::Build(const GeometricShredder& config) {
  float psi = config.psi();
  if (psi < 0 || psi > 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Psi is not in [0, 1] in GeometricShredder: ", config.DebugString()));
  }

  ASSIGN_OR_RETURN(
      std::vector<const google::protobuf::FieldDescriptor*> randomness_field,
      GetFieldFromProto(LabelerEvent().GetDescriptor(),
                        config.randomness_field()));
  if (randomness_field.back()->cpp_type() !=
      google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
    return absl::InvalidArgumentError(absl::StrCat(
        "randomness_field type is not uint64 in GeometricShredder: ",
        config.DebugString()));
  }

  ASSIGN_OR_RETURN(
      std::vector<const google::protobuf::FieldDescriptor*> target_field,
      GetFieldFromProto(LabelerEvent().GetDescriptor(), config.target_field()));
  if (target_field.back()->cpp_type() !=
      google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
    return absl::InvalidArgumentError(
        absl::StrCat("target_field type is not uint64 in GeometricShredder: ",
                     config.DebugString()));
  }

  return absl::make_unique<GeometricShredderImpl>(
      psi, std::move(randomness_field), std::move(target_field),
      config.random_seed());
}

absl::Status GeometricShredderImpl::Update(LabelerEvent& event) const {
  ASSIGN_OR_RETURN(uint64_t shred_hash, ShredHash(event));

  if (shred_hash == 0) {
    return absl::OkStatus();
  }

  ProtoFieldValue<uint64_t> target_field_value =
      GetValueFromProto<uint64_t>(event, target_field_);
  if (!target_field_value.is_set) {
    return absl::InvalidArgumentError(
        "The target field is not set in the event.");
  }
  uint64_t target_value = target_field_value.value;

  std::string full_seed =
      absl::StrFormat("%d-shred-%d-%s", target_value, shred_hash, random_seed_);

  uint64_t shred = util::Fingerprint64(full_seed);

  SetValueToProto<uint64_t>(event, target_field_, shred);

  return absl::OkStatus();
}

absl::StatusOr<uint64_t> GeometricShredderImpl::ShredHash(
    const LabelerEvent& event) const {
  // No shredding.
  if (psi_ == 0.0f) {
    return 0;
  }

  ProtoFieldValue<uint64_t> randomness_field_value =
      GetValueFromProto<uint64_t>(event, randomness_field_);
  if (!randomness_field_value.is_set) {
    return absl::InvalidArgumentError(
        "The randomness field is not set in the event.");
  }
  uint64_t randomness_value = randomness_field_value.value;

  // Certain shredding if randomness_value is not 0.
  if (psi_ == 1.0f) {
    return randomness_value;
  }

  // shred_hash = Floor(ExpHash(randomness_value) / (- Log(psi)))
  double exp_hash = ExpHash(std::to_string(randomness_value));
  return static_cast<uint64_t>(std::floor(exp_hash / (-std::log(psi_))));
}

}  // namespace wfa_virtual_people
