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

#include <memory>
#include <string>

#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "riegeli/brotli/brotli_reader.h"
#include "riegeli/brotli/brotli_writer.h"
#include "riegeli/bytes/read_all.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "wfa/panelmatch/common/compression/compressor.h"

namespace wfa::panelmatch {
namespace {
using ::riegeli::BrotliDictionary;
using ::riegeli::BrotliReader;
using ::riegeli::BrotliReaderBase;
using ::riegeli::BrotliWriter;
using ::riegeli::BrotliWriterBase;
using ::riegeli::StringReader;
using ::riegeli::StringWriter;

BrotliWriterBase::Options MakeWriterOptions(absl::string_view dictionary) {
  return BrotliWriterBase::Options()
      .set_compression_level(BrotliWriterBase::Options::kMaxCompressionLevel)
      .set_dictionary(BrotliDictionary().add_raw(dictionary));
}

BrotliReaderBase::Options MakeReaderOptions(absl::string_view dictionary) {
  return BrotliReaderBase::Options().set_dictionary(
      BrotliDictionary().add_raw(dictionary));
}

class Brotli : public Compressor {
 public:
  explicit Brotli(absl::string_view dictionary)
      : writer_options_(MakeWriterOptions(dictionary)),
        reader_options_(MakeReaderOptions(dictionary)) {}

  absl::StatusOr<std::string> Compress(
      absl::string_view uncompressed_data) const override {
    std::string result;
    BrotliWriter brotli_writer(StringWriter<std::string *>(&result),
                               writer_options_);
    if (!brotli_writer.Write(uncompressed_data) || !brotli_writer.Close()) {
      return brotli_writer.status();
    }
    return result;
  }

  absl::StatusOr<std::string> Decompress(
      absl::string_view compressed_data) const override {
    std::string result;
    absl::Status status = riegeli::ReadAll(
        BrotliReader(StringReader<absl::string_view *>(&compressed_data),
                     reader_options_),
        result);
    if (status.ok()) {
      return result;
    } else {
      return status;
    }
  }

 private:
  BrotliWriterBase::Options writer_options_;
  BrotliReaderBase::Options reader_options_;
};

}  // namespace

std::unique_ptr<Compressor> BuildBrotliCompressor(
    absl::string_view dictionary) {
  return absl::make_unique<Brotli>(dictionary);
}

}  // namespace wfa::panelmatch
