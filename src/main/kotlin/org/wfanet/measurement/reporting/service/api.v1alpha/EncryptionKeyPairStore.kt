// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Option

interface EncryptionKeyPairStore {
  fun getPrivateKey(publicKey: ByteString): PrivateKeyHandle? {
    return null
  }
}

// TODO(@renjiez): Move this class to deploy folder
class InMemoryEncryptionKeyPairStore : EncryptionKeyPairStore {
  @ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Files of encryption key pairs")
  private lateinit var encryptionKeyPairs: List<EncryptionKeyPair>

  class EncryptionKeyPair {
    @Option(
      names = ["--encryption-public-key-file"],
      description = ["File of MeasurementConsumer's encryption public key"],
      required = true
    )
    private lateinit var publicKeyFile: File

    val publicKey: ByteString by lazy { publicKeyFile.readByteString() }

    @Option(
      names = ["--encryption-private-key-file"],
      description = ["File of MeasurementConsumer's encryption private key"],
      required = true
    )
    private lateinit var privateKeyFile: File

    // TODO(@renjiez): Move loadPrivateKey out from testing package
    val privateKeyHandle: PrivateKeyHandle by lazy { loadPrivateKey(privateKeyFile) }
  }

  private val map: Map<ByteString, PrivateKeyHandle> by lazy {
    encryptionKeyPairs.associate { it.publicKey to it.privateKeyHandle }
  }

  override fun getPrivateKey(publicKey: ByteString): PrivateKeyHandle? {
    return map[publicKey]
  }
}
