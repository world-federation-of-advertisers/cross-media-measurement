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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_GEOMETRIC_SHREDDER_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_GEOMETRIC_SHREDDER_IMPL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"

namespace wfa_virtual_people {

// This is to update the target field using the shred value. The shred value is
// computed using the current target field value, the randomness field value,
// the geometric shredding parameter psi, and the random seed.
// The details can be found in the Colab
// https://github.com/world-federation-of-advertisers/virtual_people_examples/blob/main/notebooks/Geometric_Shredding.ipynb
class GeometricShredderImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when any of the following happens:
  //   @config.psi is not in [0, 1].
  //   @config.randomness_field does not refer to a valid field.
  //   @config.randomness_field does not refer to a uint64 field.
  //   @config.target_field does not refer to a valid field.
  //   @config.target_field does not refer to a uint64 field.
  static absl::StatusOr<std::unique_ptr<GeometricShredderImpl>> Build(
      const GeometricShredder& config);

  explicit GeometricShredderImpl(
      float psi,
      std::vector<const google::protobuf::FieldDescriptor*>&& randomness_field,
      std::vector<const google::protobuf::FieldDescriptor*>&& target_field,
      absl::string_view random_seed)
      : psi_(psi),
        randomness_field_(std::move(randomness_field)),
        target_field_(std::move(target_field)),
        random_seed_(random_seed) {}

  GeometricShredderImpl(const GeometricShredderImpl&) = delete;
  GeometricShredderImpl& operator=(const GeometricShredderImpl&) = delete;

  // Updates the field referred by target_field_ in @event with the shred value.
  // Returns error if the randomness field or target field is not set.
  absl::Status Update(LabelerEvent& event) const override;

 private:
  // Compute the shred hash.
  absl::StatusOr<uint64_t> ShredHash(const LabelerEvent& event) const;

  // The shredding probability parameter psi, which corresponds to the success
  // probability parameter of geometric distribution as p = 1 − psi.
  float psi_;
  // The descriptors of the field in LabelerEvent, which provides the randomness
  // for the geometric shredding.
  std::vector<const google::protobuf::FieldDescriptor*> randomness_field_;
  // The descriptors of the field in LabelerEvent, which is to be updated by
  // the shred value.
  std::vector<const google::protobuf::FieldDescriptor*> target_field_;
  // The seed used to generate the shred hash output.
  std::string random_seed_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_GEOMETRIC_SHREDDER_IMPL_H_
