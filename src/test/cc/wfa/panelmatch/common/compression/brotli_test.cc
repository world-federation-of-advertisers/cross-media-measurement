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
#include "wfa/panelmatch/common/compression/compressor.h"

namespace wfa::panelmatch {
namespace {
using ::testing::ElementsAreArray;
using ::testing::IsNull;
using ::testing::Not;
using ::testing::SizeIs;

TEST(BrotliTest, RoundTrip) {
  std::string dictionary = "abcde";

  std::unique_ptr<Compressor> brotli = BuildBrotliCompressor(dictionary);
  ASSERT_THAT(brotli, Not(IsNull()));

  // The last item should take up the least amount of space because of the
  // dictionary. This shows the dictionary is used because the second item takes
  // up more space than the first and last, despite the similar pattern.
  std::vector<std::string> items = {
      "some first item", "123451234512345 1234512345",
      "this should take up even more space", "abcdeabcdeabcde abcdeabcde"};

  std::vector<std::string> compressed_items;
  for (const std::string& item : items) {
    ASSERT_OK_AND_ASSIGN(compressed_items.emplace_back(),
                         brotli->Compress(item));
  }

  EXPECT_LT(compressed_items[3].size(), compressed_items[0].size());
  EXPECT_LT(compressed_items[0].size(), compressed_items[1].size());
  EXPECT_LT(compressed_items[1].size(), compressed_items[2].size());

  std::vector<std::string> decompressed_items;
  for (const std::string& item : compressed_items) {
    ASSERT_OK_AND_ASSIGN(decompressed_items.emplace_back(),
                         brotli->Decompress(item));
  }
  EXPECT_THAT(decompressed_items, ElementsAreArray(items));
}

}  // namespace
}  // namespace wfa::panelmatch
