// Copyright 2020 The Cross-Media Measurement Authors
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

#include "wfa/measurement/common/string_block_sorter.h"

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace wfa::measurement::common {
namespace {

TEST(SortStringByBlock, SortByCharShouldWork) {
  std::string data = "564738192";
  auto result = SortStringByBlock<1>(data);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(data, "123456789");
}

TEST(SortStringByBlock, SortByBlockShouldWork) {
  std::string data = absl::StrCat("4something", "2helloxxx2", "2helloxxx1",
                                  "3worldyyyy", "1codingzzz");
  auto result = SortStringByBlock<10>(data);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(data, absl::StrCat("1codingzzz", "2helloxxx1", "2helloxxx2",
                               "3worldyyyy", "4something"));
}

TEST(SortStringByBlock, EmptyStringShouldDoNothing) {
  std::string data = "";
  auto result = SortStringByBlock<3>(data);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(data, "");
}

TEST(SortStringByBlock, IncompatibleSizeShouldThrow) {
  std::string data = "1234567";
  auto result = SortStringByBlock<3>(data);

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(data, "1234567");  // Sort failed, data is not changed.
}

}  // namespace
}  // namespace wfa::measurement::common