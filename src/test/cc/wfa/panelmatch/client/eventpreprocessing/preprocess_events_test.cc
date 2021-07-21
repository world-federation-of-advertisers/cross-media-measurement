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

#include "wfa/panelmatch/client/eventpreprocessing/preprocess_events.h"

#include <string>

#include "absl/base/port.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "gtest/gtest.h"
#include "wfa/panelmatch/client/eventpreprocessing/preprocess_events.pb.h"

namespace wfa::panelmatch::client {
namespace {
using ::testing::ContainerEq;
using ::testing::Eq;
using ::testing::Ne;
using ::testing::Not;
using ::testing::Pointwise;

TEST(PreprocessEventsTest, ReturnsUnimplemented) {
  std::string test_id = "random-id-1";
  std::string test_data = "random-data-1";
  std::string test_crypto_key = "random-crypto_key-1";
  std::string test_pepper = "random-pepper-1";

  PreprocessEventsRequest test_request;
  PreprocessEventsRequest::UnprocessedEvent* unprocessed_event =
      test_request.add_unprocessed_events();
  unprocessed_event->set_id(test_id);
  unprocessed_event->set_data(test_data);

  test_request.set_crypto_key(test_crypto_key);
  test_request.set_pepper(test_pepper);

  auto test_response = PreprocessEvents(test_request);
  EXPECT_THAT(test_response.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));
}
}  // namespace
}  // namespace wfa::panelmatch::client
