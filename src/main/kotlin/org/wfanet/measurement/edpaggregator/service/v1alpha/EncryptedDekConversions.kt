/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek as InternalEncryptedDek
import org.wfanet.measurement.internal.edpaggregator.encryptedDek as internalEncryptedDek

/**
 * Conversions between the internal [InternalEncryptedDek] and public [EncryptedDek] messages.
 *
 * Every internal proto DEK field is typed as the internal [InternalEncryptedDek] (matching its
 * Spanner column); conversion to the public v1alpha [EncryptedDek] happens only at the v1alpha
 * service boundary. Shared by all services that surface a DEK (RankIndexBlob, PoolAssignmentJob,
 * RawImpressionUploadModelLine).
 */

/** Converts an internal [InternalEncryptedDek] to a public [EncryptedDek]. */
fun InternalEncryptedDek.toPublic(): EncryptedDek {
  val source = this
  return encryptedDek {
    kekUri = source.kekUri
    typeUrl = source.typeUrl
    protobufFormat = source.protobufFormat.toPublicProtobufFormat()
    ciphertext = source.ciphertext
  }
}

/** Converts a public [EncryptedDek] to an internal [InternalEncryptedDek]. */
fun EncryptedDek.toInternal(): InternalEncryptedDek {
  val source = this
  return internalEncryptedDek {
    kekUri = source.kekUri
    typeUrl = source.typeUrl
    protobufFormat = source.protobufFormat.toInternalProtobufFormat()
    ciphertext = source.ciphertext
  }
}

private fun InternalEncryptedDek.ProtobufFormat.toPublicProtobufFormat():
  EncryptedDek.ProtobufFormat {
  return when (this) {
    InternalEncryptedDek.ProtobufFormat.BINARY -> EncryptedDek.ProtobufFormat.BINARY
    InternalEncryptedDek.ProtobufFormat.JSON -> EncryptedDek.ProtobufFormat.JSON
    InternalEncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED ->
      EncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED
    InternalEncryptedDek.ProtobufFormat.UNRECOGNIZED -> error("Unrecognized protobuf format")
  }
}

private fun EncryptedDek.ProtobufFormat.toInternalProtobufFormat():
  InternalEncryptedDek.ProtobufFormat {
  return when (this) {
    EncryptedDek.ProtobufFormat.BINARY -> InternalEncryptedDek.ProtobufFormat.BINARY
    EncryptedDek.ProtobufFormat.JSON -> InternalEncryptedDek.ProtobufFormat.JSON
    EncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED ->
      InternalEncryptedDek.ProtobufFormat.PROTOBUF_FORMAT_UNSPECIFIED
    EncryptedDek.ProtobufFormat.UNRECOGNIZED -> error("Unrecognized protobuf format")
  }
}
