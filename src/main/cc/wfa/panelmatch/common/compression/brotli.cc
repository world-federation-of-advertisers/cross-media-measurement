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

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "riegeli/brotli/brotli_reader.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "wfa/panelmatch/common/compression/compression.pb.h"

namespace wfa::panelmatch {
using ::riegeli::BrotliReader;
using ::riegeli::BrotliReaderBase;
using ::riegeli::BrotliWriter;
using ::riegeli::BrotliWriterBase;
using ::riegeli::StringReader;
using ::riegeli::StringWriter;

absl::StatusOr<CompressResponse> BrotliCompress(
    const CompressRequest& request) {
  auto options = BrotliWriterBase::Options()
                     .set_compression_level(
                         BrotliWriterBase::Options::kMaxCompressionLevel)
                     .set_dictionaries(BrotliWriterBase::Dictionaries().add_raw(
                         request.dictionary().contents()));

  CompressResponse response;
  for (const std::string& uncompressed_data : request.uncompressed_data()) {
    BrotliWriter brotli_writer(StringWriter(response.add_compressed_data()),
                               options);
    if (!brotli_writer.Write(uncompressed_data) || !brotli_writer.Close()) {
      return brotli_writer.status();
    }
  }

  return response;
}

absl::StatusOr<DecompressResponse> BrotliDecompress(
    const DecompressRequest& request) {
  auto options = BrotliReaderBase::Options().set_dictionaries(
      BrotliReaderBase::Dictionaries().add_raw_unowned(
          request.dictionary().contents()));

  DecompressResponse response;
  for (const std::string& compressed_data : request.compressed_data()) {
    BrotliReader brotli_reader(StringReader(&compressed_data), options);
    if (!brotli_reader.ReadAll(*response.add_decompressed_data()) ||
        !brotli_reader.Close()) {
      return brotli_reader.status();
    }
  }

  return response;
}
}  // namespace wfa::panelmatch
