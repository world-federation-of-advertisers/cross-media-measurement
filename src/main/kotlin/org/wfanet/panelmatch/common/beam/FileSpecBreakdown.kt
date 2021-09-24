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

package org.wfanet.panelmatch.common.beam

private val FILE_SPEC_PATTERN = "^(.+)/([^/]+)-\\*-of-(\\d+)$".toRegex()

internal class FileSpecBreakdown(fileSpecUri: String) {
  val directoryUri: String
  val prefix: String
  val shardCount: Int

  init {
    val matchResult =
      requireNotNull(FILE_SPEC_PATTERN.matchEntire(fileSpecUri)) {
        "Invalid fileSpec string: $fileSpecUri"
      }
    directoryUri = matchResult.groupValues[1]
    prefix = matchResult.groupValues[2]
    shardCount = matchResult.groupValues[3].toInt()
  }
}
