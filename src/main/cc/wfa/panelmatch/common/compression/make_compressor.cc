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

#include "wfa/panelmatch/common/compression/make_compressor.h"

#include <memory>

#include "absl/status/statusor.h"
#include "wfa/panelmatch/common/compression/brotli.h"
#include "wfa/panelmatch/common/compression/compression.pb.h"
#include "wfa/panelmatch/common/compression/compressor.h"
#include "wfa/panelmatch/common/compression/no_op.h"

namespace wfa::panelmatch {

absl::StatusOr<std::unique_ptr<Compressor>> MakeCompressor(
    const CompressionParameters& parameters) {
  switch (parameters.type_case()) {
    case CompressionParameters::TypeCase::TYPE_NOT_SET:
      return absl::InvalidArgumentError("Missing CompressionParameters");
    case CompressionParameters::TypeCase::kBrotli:
      return BuildBrotliCompressor(parameters.brotli().dictionary());
    case CompressionParameters::TypeCase::kUncompressed:
      return BuildNoOpCompressor();
  }
}

}  // namespace wfa::panelmatch
