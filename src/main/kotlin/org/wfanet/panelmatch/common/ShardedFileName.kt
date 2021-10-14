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

package org.wfanet.panelmatch.common

private val SPEC_REGEX = """^([\w-.]+)-(\*|\?+)-of-(\d+)$""".toRegex()

/**
 * Represents a set of filenames with format "baseName-0001-of-9876", "baseName-0002-of-9876", etc.
 *
 * A sharded file spec has one of the following formats:
 *
 * 1. "baseName-*-of-X", where X is a number
 * 2. "baseName-????-of-X", where X is a number and the number of question marks is equal to the
 * number of base-10 digits in X.
 */
data class ShardedFileName(val spec: String) {
  constructor(baseName: String, shardCount: Int) : this("$baseName-*-of-$shardCount")

  val baseName: String
  val shardCount: Int

  val fileNames: Sequence<String>
    get() = (0 until shardCount).asSequence().map(this::fileNameForShard)

  init {
    val matchResult = requireNotNull(SPEC_REGEX.matchEntire(spec)) { "Invalid spec: $spec" }
    val matches = matchResult.groupValues
    check(!matches[2].startsWith("?") || matches[2].length == matches[3].length) {
      "Unexpected number of question marks: $spec"
    }

    baseName = matches[1]
    shardCount = matches[3].toInt()
  }

  fun fileNameForShard(i: Int): String {
    require(i in 0 until shardCount)
    val digits = shardCount.toString().length
    return "$baseName-%0${digits}d-of-$shardCount".format(i)
  }
}
