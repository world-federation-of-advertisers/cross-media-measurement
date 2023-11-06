// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api

enum class Version(val string: String) {
  V2_ALPHA("v2alpha");

  override fun toString(): String = string

  companion object {
    fun fromString(string: String): Version {
      return fromStringOrNull(string)
        ?: throw IllegalArgumentException("$string is not a valid Version string")
    }

    fun fromStringOrNull(string: String): Version? {
      return values().find { it.string == string }
    }
  }
}
