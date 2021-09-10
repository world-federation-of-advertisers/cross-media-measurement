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
#include "riegeli/base/base.h"
#include "riegeli/base/status.h"
#include "riegeli/brotli/brotli_reader.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "wfa/panelmatch/common/compression/compression.pb.h"

namespace wfa::panelmatch {
using ::riegeli::Annotate;
using ::riegeli::BrotliReader;
using ::riegeli::BrotliReaderBase;
using ::riegeli::BrotliWriter;
using ::riegeli::BrotliWriterBase;
using ::riegeli::FlushType;
using ::riegeli::StringReader;
using ::riegeli::StringWriter;

absl::StatusOr<CompressResponse> BrotliCompress(
    const CompressRequest& request) {
  StringWriter<std::string*> string_writer;

  auto options = BrotliWriterBase::Options()
                     .set_compression_level(
                         BrotliWriterBase::Options::kMaxCompressionLevel)
                     .set_dictionaries(BrotliWriterBase::Dictionaries().add_raw(
                         request.dictionary()));
  BrotliWriter<StringWriter<std::string*>*> brotli_writer;

  CompressResponse response;

  for (const std::string& uncompressed_data : request.uncompressed_data()) {
    string_writer.Reset(response.add_compressed_data());
    brotli_writer.Reset(&string_writer, options);
    if (!brotli_writer.Write(uncompressed_data)) {
      return Annotate(brotli_writer.status(), "Failed on write");
    }
    if (!brotli_writer.Flush(FlushType::kFromProcess)) {
      return Annotate(brotli_writer.status(), "Failed on flush");
    }
    if (!string_writer.Flush(FlushType::kFromProcess)) {
      return Annotate(string_writer.status(), "Failed on flushing string");
    }
  }

  brotli_writer.Close();
  string_writer.Close();

  return response;
}

absl::StatusOr<DecompressResponse> BrotliDecompress(
    const DecompressRequest& request) {
  StringReader<absl::string_view> string_reader;

  auto options = BrotliReaderBase::Options().set_dictionaries(
      BrotliReaderBase::Dictionaries().add_raw(request.dictionary()));
  BrotliReader<StringReader<absl::string_view>*> brotli_reader;

  DecompressResponse response;
  for (const std::string& compressed_data : request.compressed_data()) {
    string_reader.Reset(compressed_data);
    brotli_reader.Reset(&string_reader, options);
    if (!brotli_reader.ReadAll(*response.add_decompressed_data())) {
      return Annotate(brotli_reader.status(), "Failed to read");
    }
  }

  return response;
}
}  // namespace wfa::panelmatch
