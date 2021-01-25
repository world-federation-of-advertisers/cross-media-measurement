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

package org.wfanet.measurement.duchy.testing

import com.google.protobuf.ByteString
import java.nio.file.Files
import java.nio.file.Paths
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.DuchyPublicKeyConfig
import org.wfanet.measurement.duchy.DuchyPublicKeys

val DUCHY_PUBLIC_KEY_CONFIG = loadDuchyPublicKeyConfig()
val DUCHY_PUBLIC_KEYS = DuchyPublicKeys(DUCHY_PUBLIC_KEY_CONFIG)
val DUCHY_IDS: Set<String> = DUCHY_PUBLIC_KEYS.latest.keys

val DUCHY_A_SECRET_KEY = byteStringOf(
  0x05, 0x7b, 0x22, 0xef, 0x9c, 0x4e, 0x96, 0x26, 0xc2, 0x2c, 0x13, 0xda, 0xed, 0x13, 0x63, 0xa1,
  0xe6, 0xa5, 0xb3, 0x09, 0xa9, 0x30, 0x40, 0x9f, 0x8d, 0x13, 0x1f, 0x96, 0xea, 0x2f, 0xa8, 0x88
)
val DUCHY_B_SECRET_KEY = byteStringOf(
  0x31, 0xcc, 0x32, 0xe7, 0xcd, 0x53, 0xff, 0x24, 0xf2, 0xb6, 0x4a, 0xe8, 0xc5, 0x31, 0x09, 0x9a,
  0xf9, 0x86, 0x7e, 0xbf, 0x5d, 0x9a, 0x65, 0x9f, 0x74, 0x24, 0x59, 0x94, 0x7c, 0xaa, 0x29, 0xb0
)
val DUCHY_C_SECRET_KEY = byteStringOf(
  0x33, 0x8c, 0xce, 0x03, 0x06, 0x41, 0x6b, 0x70, 0xe9, 0x01, 0x43, 0x6c, 0xb9, 0xec, 0xa5, 0xac,
  0x75, 0x8e, 0x8f, 0xf4, 0x1d, 0x7b, 0x58, 0xda, 0xba, 0xdf, 0x87, 0x26, 0x60, 0x8c, 0xa6, 0xcc
)

/** [Map] of Duchy ID to ElGamal secret key. */
val DUCHY_SECRET_KEYS: Map<String, ByteString> = DUCHY_IDS.sorted()
  .zip(listOf(DUCHY_A_SECRET_KEY, DUCHY_B_SECRET_KEY, DUCHY_C_SECRET_KEY))
  .toMap()

private fun loadDuchyPublicKeyConfig(): DuchyPublicKeyConfig {
  val runfilesRelativePath =
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "k8s",
      "duchy_public_key_config.textproto"
    )
  val path = checkNotNull(getRuntimePath(runfilesRelativePath))

  return Files.newBufferedReader(path).use { reader ->
    parseTextProto(reader, DuchyPublicKeyConfig.getDefaultInstance())
  }
}
