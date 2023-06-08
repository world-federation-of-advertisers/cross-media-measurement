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

#include "wfa/panelmatch/common/compression/no_op.h"

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/panelmatch/common/compression/compressor.h"

namespace wfa::panelmatch {
namespace {
class NoOpCompressor : public Compressor {
 public:
  NoOpCompressor() = default;

  absl::StatusOr<std::string> Compress(
      absl::string_view uncompressed_data) const override {
    return std::string(uncompressed_data);
  }

  absl::StatusOr<std::string> Decompress(
      absl::string_view compressed_data) const override {
    return std::string(compressed_data);
  }
};
}  // namespace

std::unique_ptr<Compressor> BuildNoOpCompressor() {
  return absl::make_unique<NoOpCompressor>();
}

}  // namespace wfa::panelmatch
