// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

/** A exception generated with a results fulfiller is unable to read impression. */
class ImpressionReadException(val blobKey: String, val code: Code, message: String? = null) :
  Exception(message) {
  enum class Code {
    /** Blob was not found */
    BLOB_NOT_FOUND,

    /** Unable to parse the blob contents */
    INVALID_FORMAT,
  }
}
