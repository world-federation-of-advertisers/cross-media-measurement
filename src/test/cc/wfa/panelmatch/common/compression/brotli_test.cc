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

#include "wfa/panelmatch/common/compression/brotli.h"

#include <string>

#include "absl/strings/string_view.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "wfa/panelmatch/common/compression/compression.pb.h"

namespace wfa::panelmatch {
namespace {
using ::testing::ElementsAreArray;
using ::testing::SizeIs;

TEST(BrotliTest, RoundTrip) {
  CompressRequest compress_request;
  compress_request.mutable_dictionary()->set_contents("abcde");

  // The last item should take up the least amount of space because of the
  // dictionary. This shows the dictionary is used because the second item takes
  // up more space than the first and last, despite the similar pattern.
  compress_request.add_uncompressed_data("some first item");
  compress_request.add_uncompressed_data("123451234512345 1234512345");
  compress_request.add_uncompressed_data("this should take up even more space");
  compress_request.add_uncompressed_data("abcdeabcdeabcde abcdeabcde");

  EXPECT_EQ(BrotliCompress(compress_request).status(), absl::OkStatus());
  ASSERT_OK_AND_ASSIGN(CompressResponse compress_response,
                       BrotliCompress(compress_request));

  ASSERT_THAT(compress_response.compressed_data(), SizeIs(4));

  std::vector<int> sizes;
  for (const std::string& item : compress_response.compressed_data()) {
    sizes.push_back(item.size());
  }
  EXPECT_LT(sizes[3], sizes[0]);
  EXPECT_LT(sizes[0], sizes[1]);
  EXPECT_LT(sizes[1], sizes[2]);

  DecompressRequest decompress_request;
  *decompress_request.mutable_dictionary() = compress_request.dictionary();
  decompress_request.mutable_compressed_data()->CopyFrom(
      compress_response.compressed_data());

  ASSERT_OK_AND_ASSIGN(DecompressResponse decompress_response,
                       BrotliDecompress(decompress_request));

  EXPECT_THAT(decompress_response.decompressed_data(),
              ElementsAreArray(compress_request.uncompressed_data()));
}

}  // namespace
}  // namespace wfa::panelmatch
