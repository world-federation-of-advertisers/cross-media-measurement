// Copyright 2020 The Measurement System Authors
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

#ifndef WFA_MEASUREMENT_COMMON_STRING_BLOCK_SORTER_H_
#define WFA_MEASUREMENT_COMMON_STRING_BLOCK_SORTER_H_

#include <algorithm>
#include <cstring>

#include "absl/status/status.h"

namespace wfa {
namespace measurement {
namespace common {

namespace internal {

// A block of data, which can be either a sketch register or (flag, cout) pair.
// Used to sort the data by blocks, no actual DataBlock object is actually
// created.
template <size_t kBlockSize>
struct DataBlock {
  DataBlock() = delete;

  unsigned char data[kBlockSize];
  bool operator<(const DataBlock& rhs) const {
    static_assert(sizeof(DataBlock<kBlockSize>) == kBlockSize, "Invalid size");
    static_assert(alignof(DataBlock<kBlockSize>) == 1, "Invalid alignment");
    return memcmp(data, rhs.data, kBlockSize) < 0;
  }
};

}  // namespace internal

// Sort the string by blocks.
template <size_t kBlockSize>
absl::Status SortStringByBlock(std::string& data) {
  if (data.length() % kBlockSize != 0) {
    return absl::InvalidArgumentError(
        "Data size is not divisible by the block size.");
  }
  std::sort(reinterpret_cast<internal::DataBlock<kBlockSize>*>(data.data()),
            reinterpret_cast<internal::DataBlock<kBlockSize>*>(data.data() +
                                                               data.length()));
  return absl::OkStatus();
}

template <>
absl::Status SortStringByBlock<0>(std::string& data) = delete;

}  // namespace common
}  // namespace measurement
}  // namespace wfa

#endif  // WFA_MEASUREMENT_COMMON_STRING_BLOCK_SORTER_H_