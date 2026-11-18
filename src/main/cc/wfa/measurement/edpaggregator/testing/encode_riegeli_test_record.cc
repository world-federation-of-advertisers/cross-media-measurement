// Copyright 2026 The Cross-Media Measurement Authors
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

#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>

#include "riegeli/bytes/string_writer.h"
#include "riegeli/records/record_writer.h"

// Encodes one stdin record as an uncompressed Riegeli file on stdout.
int main() {
  const std::string record((std::istreambuf_iterator<char>(std::cin)),
                           std::istreambuf_iterator<char>());
  if (record.empty()) return EXIT_FAILURE;
  std::string encoded;
  riegeli::RecordWriter<riegeli::StringWriter<>> writer(
      riegeli::StringWriter<>(&encoded),
      riegeli::RecordWriterBase::Options().set_uncompressed());
  if (!writer.WriteRecord(record) || !writer.Close()) {
    std::cerr << "Unable to encode test record" << std::endl;
    return EXIT_FAILURE;
  }
  std::cout.write(encoded.data(), encoded.size());
  return std::cout ? EXIT_SUCCESS : EXIT_FAILURE;
}
