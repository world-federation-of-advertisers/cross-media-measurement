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

package org.wfanet.panelmatch.common.storage.testing

import org.wfanet.measurement.common.crypto.PrivateKeyStore as CryptoPrivateKeyStore
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle

internal class FakePrivateKeyStore : CryptoPrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle> {
  override suspend fun read(keyId: TinkKeyId): TinkPrivateKeyHandle? {
    TODO("Not yet implemented")
  }

  override suspend fun write(privateKey: TinkPrivateKeyHandle): String {
    TODO("Not yet implemented")
  }
}
