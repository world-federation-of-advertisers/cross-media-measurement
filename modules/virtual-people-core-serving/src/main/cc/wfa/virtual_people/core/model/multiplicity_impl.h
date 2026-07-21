// Copyright 2022 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MULTIPLICITY_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MULTIPLICITY_IMPL_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// Extracts multiplicity value from a field.
struct MultiplicityFromField {
  // The field descriptor.
  std::vector<const google::protobuf::FieldDescriptor*> field_descriptor;

  // The function to extract the multiplicity value.
  std::function<absl::StatusOr<double>(
      const LabelerEvent&,
      const std::vector<const google::protobuf::FieldDescriptor*>&)>
      get_value_function;
};

// monostate: Invalid extractor.
// double: Explicit multiplicity value.
// MultiplicityFromField: Extracts multiplicity value from a field.
using MultiplicityExtractor =
    absl::variant<std::monostate, double, MultiplicityFromField>;

// The implementation of Multiplicity protobuf.
class MultiplicityImpl {
 public:
  // Always use Build to get a MultiplicityImpl object. Users should not
  // call the constructor directly.
  //
  // Returns error status if any of the following happens:
  //   @config.multiplicity_ref is not set.
  //   @config.expected_multiplicity_field is set, but is not a valid field,
  //     or the field type is not one of int32/int64/uint32/uint64/float/double.
  //   @config.person_index_field is not set, or is not a valid field, or the
  //     field type is not one of int32/int64/uint32/uint64.
  //   @config.max_value is not set.
  //   @config.cap_at_max is not set.
  //   @config.random_seed is not set.
  static absl::StatusOr<std::unique_ptr<MultiplicityImpl>> Build(
      const Multiplicity& config);

  enum class CapMultiplicityAtMax { kNo, kYes };

  explicit MultiplicityImpl(
      MultiplicityExtractor& multiplicity_extractor,
      CapMultiplicityAtMax cap_at_max, double max_value,
      std::vector<const google::protobuf::FieldDescriptor*>&&
          person_index_field,
      absl::string_view random_seed);

  MultiplicityImpl(const MultiplicityImpl&) = delete;
  MultiplicityImpl& operator=(const MultiplicityImpl&) = delete;

  // Computes multiplicity for @event.
  // 1. Extracts the expected_multiplicity. Returns error status if
  //    @cap_at_max_ = kNo and expected_multiplicity > @max_value_.
  // 2. Pseudorandomly generates an integer value that is either
  //    floor(expected_multiplicity) or floor(expected_multiplicity) + 1, with
  //    expectation = expected_multiplicity.
  // For example, with expected_multiplicity = 1.4, this returns either 1 or 2,
  // with 60% and 40% probabilities, respectively.
  // This always returns the same result for the same event.
  absl::StatusOr<int> ComputeEventMultiplicity(const LabelerEvent& event) const;

  // Gets the person_index field descriptor.
  const std::vector<const google::protobuf::FieldDescriptor*>&
  PersonIndexFieldDescriptor() const;

  // Gets fingerprint for @index using @input and @random_seed_.
  // Returns @input as is for index = 0.
  uint64_t GetFingerprintForIndex(uint64_t input, int index) const;

 private:
  // Extractor for expected multiplicity.
  MultiplicityExtractor multiplicity_extractor_;

  // When expected multiplicity > max_value_, cap at max_value_ if cap_at_max_
  // is kYes, else return error status.
  CapMultiplicityAtMax cap_at_max_;
  double max_value_;

  // The field to set person index in.
  std::vector<const google::protobuf::FieldDescriptor*> person_index_field_;

  // The random seed. It is used to
  // - compute multiplicity for a given event and
  // - compute fingerprint for cloned event
  std::string random_seed_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MULTIPLICITY_IMPL_H_
