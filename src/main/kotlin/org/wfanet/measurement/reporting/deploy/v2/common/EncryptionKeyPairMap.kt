/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.common

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import picocli.CommandLine.Option

class EncryptionKeyPairMap {
  @Option(
    names = ["--key-pair-dir"],
    description = ["Path to the directory of MeasurementConsumer's encryption keys"],
  )
  private lateinit var keyFilesDirectory: File

  @Option(
    names = ["--key-pair-config-file"],
    description = ["Path to the textproto file of EncryptionKeyPairConfig that contains key pairs"],
    required = true,
  )
  private lateinit var keyPairConfigFile: File

  private fun loadKeyPairs(): Map<String, List<Pair<ByteString, PrivateKeyHandle>>> {
    val keyPairConfig =
      parseTextProto(keyPairConfigFile, encryptionKeyPairConfig {}).principalKeyPairsList
    return keyPairConfig.associate { config ->
      val keyPairs =
        config.keyPairsList.map { keyPair ->
          val publicKey = keyFilesDirectory.resolve(keyPair.publicKeyFile).readByteString()
          val privateKey = loadPrivateKey(keyFilesDirectory.resolve(keyPair.privateKeyFile))
          publicKey to privateKey
        }
      checkNotNull(config.principal) to keyPairs
    }
  }

  val keyPairs: Map<String, List<Pair<ByteString, PrivateKeyHandle>>> by lazy { loadKeyPairs() }
}
