// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model.utils

import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets
import kotlin.math.ln

/** Hash the seed to a float number. The output is in range [0, 1). */
fun floatHash(seed: String): Double {
  return Hashing.farmHashFingerprint64()
    .hashString(seed, StandardCharsets.UTF_8)
    .asLong()
    .toULong()
    .toDouble() / ULong.MAX_VALUE.toDouble()
}

/** Exponentially distributed hash. The output is a positive double float number. */
fun expHash(seed: String): Double {
  return -ln(floatHash(seed))
}
