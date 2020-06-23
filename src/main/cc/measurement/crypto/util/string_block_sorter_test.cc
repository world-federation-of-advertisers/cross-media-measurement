#include "string_block_sorter.h"

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace wfa::measurement::crypto::util {
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
}  // namespace wfa::measurement::crypto::util