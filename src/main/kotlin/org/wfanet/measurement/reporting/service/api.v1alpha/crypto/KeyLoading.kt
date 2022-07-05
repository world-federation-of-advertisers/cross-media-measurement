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

package org.wfanet.measurement.reporting.service.api.v1alpha.crypto

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import java.io.File
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle

/** Loads a private key from a cleartext binary Tink Keyset. */
fun loadPrivateKey(binaryKeyset: File): TinkPrivateKeyHandle {
  return TinkPrivateKeyHandle(CleartextKeysetHandle.read(BinaryKeysetReader.withFile(binaryKeyset)))
}

/** Loads a public key from a cleartext binary Tink Keyset. */
fun loadPublicKey(binaryKeyset: File): TinkPublicKeyHandle {
  return TinkPublicKeyHandle(KeysetHandle.readNoSecret(BinaryKeysetReader.withFile(binaryKeyset)))
}
