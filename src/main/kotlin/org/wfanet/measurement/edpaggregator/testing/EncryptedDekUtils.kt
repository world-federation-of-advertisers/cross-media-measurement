/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString

object EncryptedDekUtils {
  fun getSerializedEncryptionKey(
    kmsClient: KmsClient,
    kekUri: String,
    tinkKeyTemplateType: String,
  ): ByteString {
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    return ByteString.copyFrom(
      TinkProtoKeysetFormat.serializeEncryptedKeyset(
        keyEncryptionHandle,
        kmsClient.getAead(kekUri),
        byteArrayOf(),
      )
    )
  }
}
