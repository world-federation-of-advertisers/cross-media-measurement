/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.ByteOrder

/**
 * The salted VID hash whose rank orders a population's VIDs into frequency vector indexes.
 *
 * A VID's global frequency vector index is the rank of its hash among the hashes of every VID in
 * the population, ties broken by VID.
 */
object VidHashing {
  /** Default salt. */
  val SALT: ByteString = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

  /** Returns the FarmHash fingerprint of [vid], big-endian, followed by [salt]. */
  fun hash(vid: Long, salt: ByteString = SALT): Long {
    val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
  }
}
