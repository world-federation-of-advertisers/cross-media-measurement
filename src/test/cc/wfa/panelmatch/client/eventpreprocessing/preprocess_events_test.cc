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
#include "wfa/panelmatch/common/compression/compression.pb.h"
#include "wfa/panelmatch/protocol/crypto/event_data_preprocessor.h"

namespace wfa::panelmatch::client::eventpreprocessing {
namespace {
using ::testing::ContainerEq;
using ::testing::Eq;
using ::testing::Ne;
using ::testing::Not;
using ::testing::Pointwise;
using ::wfa::panelmatch::protocol::crypto::EventDataPreprocessor;
using ::wfa::panelmatch::protocol::crypto::ProcessedData;

PreprocessEventsRequest MakeTestRequest() {
  PreprocessEventsRequest request;
  request.set_crypto_key("some-cryptokey");
  request.set_identifier_hash_pepper("some-identifier-hash-pepper");
  request.set_hkdf_pepper("some-hkdf-pepper");
  request.mutable_compression_parameters()->mutable_uncompressed();
  return request;
}

void AppendEvent(absl::string_view id, absl::string_view data,
                 PreprocessEventsRequest& request) {
  UnprocessedEvent* event = request.add_unprocessed_events();
  *event->mutable_id() = id;
  *event->mutable_data() = data;
}

// Test using actual implementations to ensure nothing crashes
TEST(PreprocessEventsTest, ActualValues) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id", "some-data", request);

  ASSERT_OK_AND_ASSIGN(PreprocessEventsResponse processed,
                       PreprocessEvents(request));
  ASSERT_EQ(processed.processed_events_size(), 1);
  EXPECT_NE(processed.processed_events(0).encrypted_data(), "some-data");
  // TODO(@efoxepstein): decrypt encrypted_data to ensure that it's correct.
}

// Test using actual implementations to ensure nothing crashes
TEST(PreprocessEventsTest, BrotliCompression) {
  std::string id = "some-id";
  std::string data = "some-some-some-data-that-should-compress-some";

  PreprocessEventsRequest uncompressed_request = MakeTestRequest();
  AppendEvent(id, data, uncompressed_request);
  ASSERT_OK_AND_ASSIGN(PreprocessEventsResponse uncompressed_response,
                       PreprocessEvents(uncompressed_request));
  ASSERT_EQ(uncompressed_response.processed_events_size(), 1);

  PreprocessEventsRequest brotli_request = uncompressed_request;
  brotli_request.clear_compression_parameters();
  brotli_request.mutable_compression_parameters()
      ->mutable_brotli()
      ->set_dictionary("some-dictionary");
  ASSERT_OK_AND_ASSIGN(PreprocessEventsResponse brotli_response,
                       PreprocessEvents(brotli_request));
  ASSERT_EQ(brotli_response.processed_events_size(), 1);

  EXPECT_GT(uncompressed_response.ByteSizeLong(),
            brotli_response.ByteSizeLong());
}

TEST(PreprocessEventsTest, MissingIdentifierHashPepper) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id", "some-data", request);

  request.clear_identifier_hash_pepper();

  ASSERT_THAT(PreprocessEvents(request).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PreprocessEventsTest, MissingHkdfPepper) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id", "some-data", request);

  request.clear_hkdf_pepper();

  ASSERT_THAT(PreprocessEvents(request).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PreprocessEventsTest, MissingCryptokey) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id", "some-data", request);

  request.clear_crypto_key();

  ASSERT_THAT(PreprocessEvents(request).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PreprocessEventsTest, MissingUnprocessedEvents) {
  PreprocessEventsRequest request = MakeTestRequest();

  ASSERT_OK_AND_ASSIGN(PreprocessEventsResponse processed,
                       PreprocessEvents(request));
  EXPECT_EQ(processed.processed_events_size(), 0);
}

TEST(PreprocessEventsTest, MissingId) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("", "some-data", request);

  EXPECT_THAT(PreprocessEvents(request).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(PreprocessEventsTest, MissingData) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id", "", request);

  EXPECT_THAT(PreprocessEvents(request).status(), IsOk());
}

TEST(PreprocessEventsTest, MultipleUnprocessedEvents) {
  PreprocessEventsRequest request = MakeTestRequest();
  AppendEvent("some-id-1", "some-data-1", request);
  AppendEvent("some-id-2", "some-data-2", request);
  AppendEvent("some-id-3", "some-data-3", request);

  ASSERT_OK_AND_ASSIGN(PreprocessEventsResponse processed,
                       PreprocessEvents(request));
  ASSERT_EQ(processed.processed_events_size(), 3);
  EXPECT_NE(processed.processed_events(0).encrypted_data(), "some-data-1");
  EXPECT_NE(processed.processed_events(1).encrypted_data(), "some-data-2");
  EXPECT_NE(processed.processed_events(2).encrypted_data(), "some-data-3");
}
}  // namespace
}  // namespace wfa::panelmatch::client::eventpreprocessing
