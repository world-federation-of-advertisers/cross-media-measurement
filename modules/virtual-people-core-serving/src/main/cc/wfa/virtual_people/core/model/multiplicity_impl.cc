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

#include "wfa/virtual_people/core/model/multiplicity_impl.h"

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/variant.h"
#include "common_cpp/macros/macros.h"
#include "src/farmhash.h"
#include "wfa/virtual_people/common/field_filter/utils/field_util.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// Returns an integer that is either N or N + 1, where N is the integral
// part of @expectation.
// The choice is made by comparing @seed to the fractional part of @expectation.
// This is used to compute multiplicity, and @expectation should be much lower
// than int max.
absl::StatusOr<int> ComputeBimodalInteger(double expectation, uint64_t seed) {
  int integral_part = static_cast<int>(floor(expectation));
  double fractional_part = expectation - integral_part;
  uint64_t threshold = static_cast<uint64_t>(
      fractional_part *
      static_cast<double>(std::numeric_limits<uint64_t>::max()));
  if (seed < threshold) {
    return integral_part + 1;
  } else {
    return integral_part;
  }
}

template <typename ValueType>
using IsIntegerFloatType =
    absl::disjunction<IsIntegerType<ValueType>, std::is_same<ValueType, float>,
                      std::is_same<ValueType, double>>;

template <typename ValueType>
using EnableIfIntegerFloatType =
    absl::enable_if_t<IsIntegerFloatType<ValueType>::value, bool>;

// Extracts a double value from the @source field in @event.
template <typename ValueType, EnableIfIntegerFloatType<ValueType> = true>
absl::StatusOr<double> ExtractDoubleValue(
    const LabelerEvent& event,
    const std::vector<const google::protobuf::FieldDescriptor*>& source) {
  ProtoFieldValue<ValueType> field_value =
      GetValueFromProto<ValueType>(event, source);
  if (field_value.is_set) {
    return field_value.value;
  }

  return absl::InvalidArgumentError("The multiplicity field is not set.");
}

// Gets a function to extract multiplicity value from a field.
absl::StatusOr<std::function<absl::StatusOr<double>(
    const LabelerEvent&,
    const std::vector<const google::protobuf::FieldDescriptor*>&)>>
GetExtractMultiplicityFunction(
    const google::protobuf::FieldDescriptor::CppType cpp_type) {
  switch (cpp_type) {
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
      return ExtractDoubleValue<int32_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
      return ExtractDoubleValue<int64_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
      return ExtractDoubleValue<uint32_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
      return ExtractDoubleValue<uint64_t>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
      return ExtractDoubleValue<float>;
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
      return ExtractDoubleValue<double>;
    default:
      return absl::InvalidArgumentError(
          "Unsupported field type for multiplicity.");
  }
}

bool IsIntegerFieldType(
    const google::protobuf::FieldDescriptor::CppType cpp_type) {
  switch (cpp_type) {
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
    case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
      return true;
    default:
      return false;
  }

  return false;
}

absl::StatusOr<std::unique_ptr<MultiplicityImpl>> MultiplicityImpl::Build(
    const Multiplicity& config) {
  MultiplicityExtractor multiplicity_extractor;
  switch (config.multiplicity_ref_case()) {
    case Multiplicity::kExpectedMultiplicity: {
      multiplicity_extractor = config.expected_multiplicity();
      break;
    }
    case Multiplicity::kExpectedMultiplicityField: {
      ASSIGN_OR_RETURN(std::vector<const google::protobuf::FieldDescriptor*>
                           field_descriptor,
                       GetFieldFromProto(LabelerEvent().GetDescriptor(),
                                         config.expected_multiplicity_field()));
      MultiplicityFromField from_field;
      from_field.field_descriptor = std::move(field_descriptor);
      ASSIGN_OR_RETURN(from_field.get_value_function,
                       GetExtractMultiplicityFunction(
                           from_field.field_descriptor.back()->cpp_type()));
      multiplicity_extractor = std::move(from_field);
      break;
    }
    default:
      // multiplicity_ref is not set.
      return absl::InvalidArgumentError(absl::StrCat(
          "Multiplicity must set multiplicity_ref.", config.DebugString()));
  }

  if (!config.has_person_index_field()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Multiplicity must set person_index_field.", config.DebugString()));
  }
  ASSIGN_OR_RETURN(
      std::vector<const google::protobuf::FieldDescriptor*> person_index_field,
      GetFieldFromProto(LabelerEvent().GetDescriptor(),
                        config.person_index_field()));
  if (!IsIntegerFieldType(person_index_field.back()->cpp_type())) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Invalid type for person_index_field.", config.DebugString()));
  }

  if (!config.has_max_value()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Multiplicity must set max_value.", config.DebugString()));
  }
  if (!config.has_cap_at_max()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Multiplicity must set cap_at_max.", config.DebugString()));
  }
  if (!config.has_random_seed()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Multiplicity must set random_seed.", config.DebugString()));
  }

  CapMultiplicityAtMax cap_at_max = config.cap_at_max()
                                        ? CapMultiplicityAtMax::kYes
                                        : CapMultiplicityAtMax::kNo;
  return absl::make_unique<MultiplicityImpl>(
      multiplicity_extractor, cap_at_max, config.max_value(),
      std::move(person_index_field), config.random_seed());
}

MultiplicityImpl::MultiplicityImpl(
    MultiplicityExtractor& multiplicity_extractor,
    CapMultiplicityAtMax cap_at_max, double max_value,
    std::vector<const google::protobuf::FieldDescriptor*>&& person_index_field,
    absl::string_view random_seed)
    : multiplicity_extractor_(std::move(multiplicity_extractor)),
      cap_at_max_(cap_at_max),
      max_value_(max_value),
      person_index_field_(std::move(person_index_field)),
      random_seed_(random_seed) {}

absl::StatusOr<int> MultiplicityImpl::ComputeEventMultiplicity(
    const LabelerEvent& event) const {
  double expected_multiplicity = 0.0;
  if (const double* explicit_value =
          std::get_if<double>(&multiplicity_extractor_)) {
    expected_multiplicity = *explicit_value;
  } else if (const MultiplicityFromField* from_field =
                 std::get_if<MultiplicityFromField>(&multiplicity_extractor_)) {
    if (!from_field->get_value_function) {
      return absl::InternalError("Extractor has NULL get_value_function.");
    }

    if (from_field->field_descriptor.empty()) {
      return absl::InternalError("Extractor has invalid field_descriptor.");
    }

    ASSIGN_OR_RETURN(
        expected_multiplicity,
        from_field->get_value_function(event, from_field->field_descriptor));
  } else {
    return absl::InternalError("Invalid multiplicity extractor.");
  }

  if (expected_multiplicity > max_value_) {
    if (cap_at_max_ == CapMultiplicityAtMax::kYes) {
      expected_multiplicity = max_value_;
    } else {
      return absl::OutOfRangeError(absl::StrCat(
          "Expected multiplicity = ", expected_multiplicity,
          ", which exceeds the specified max value = ", max_value_));
    }
  }

  if (expected_multiplicity < 0) {
    return absl::OutOfRangeError(
        absl::StrCat("Expected multiplicity = ", expected_multiplicity,
                     ", but multiplicity must >= 0."));
  }

  uint64_t event_seed = util::Fingerprint64(
      absl::StrCat(random_seed_, event.acting_fingerprint()));
  return ComputeBimodalInteger(expected_multiplicity, event_seed);
}

const std::vector<const google::protobuf::FieldDescriptor*>&
MultiplicityImpl::PersonIndexFieldDescriptor() const {
  return person_index_field_;
}

uint64_t MultiplicityImpl::GetFingerprintForIndex(uint64_t input,
                                                  int index) const {
  if (index == 0) {
    return input;
  }

  return util::Fingerprint64(
      absl::StrCat(random_seed_, "-clone-", index, "-", input));
}

}  // namespace wfa_virtual_people
