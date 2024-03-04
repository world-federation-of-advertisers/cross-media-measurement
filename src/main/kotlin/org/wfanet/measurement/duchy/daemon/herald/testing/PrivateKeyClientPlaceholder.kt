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

package org.wfanet.measurement.duchy.daemon.herald.testing

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

private const val KEY_URI_SCHEME = "fake-kms"
private const val KEY_URI_PREFIX = "$KEY_URI_SCHEME://"
private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM")

/**
 * A placeholder of private key client used for HMSS protocol during dev.
 *
 * @TODO(@renjiez): replace the placeholder with tink key and storage by configuration.
 */
object PrivateKeyClientPlaceholder {
  fun createInstance(): PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle> {
    val storageClient = InMemoryStorageClient()
    val encryptionKey = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    val aead = encryptionKey.getPrimitive(Aead::class.java)
    val kmsClient = FakeKmsClient().also { it.setAead(KEY_URI_PREFIX, aead) }
    val tinkKeyStore = TinkKeyStore(storageClient)
    return TinkKeyStorageProvider(kmsClient).makeKmsPrivateKeyStore(tinkKeyStore, KEY_URI_PREFIX)
  }
}
