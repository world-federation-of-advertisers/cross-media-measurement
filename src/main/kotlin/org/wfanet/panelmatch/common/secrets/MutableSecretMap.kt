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

package org.wfanet.panelmatch.common.secrets

import com.google.protobuf.ByteString

/** [SecretMap] that supports insertion. */
interface MutableSecretMap : SecretMap {
  /**
   * Adds a mapping from [key] to [value].
   *
   * This must overwrite any previous value associated with [key].
   */
  suspend fun put(key: String, value: ByteString)
}
