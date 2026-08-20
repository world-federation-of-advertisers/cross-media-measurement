// Copyright 2021 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/model/utils/update_matrix_helper.h"

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::StatusIs;

TEST(SelectFromMatrixTest, NullMatchers) {
  EXPECT_THAT(SelectFromMatrix(
                  nullptr, nullptr,
                  std::vector<std::unique_ptr<DistributedConsistentHashing>>(),
                  "", LabelerEvent())
                  .status(),
              StatusIs(absl::StatusCode::kInternal, ""));
}

}  // namespace
}  // namespace wfa_virtual_people
