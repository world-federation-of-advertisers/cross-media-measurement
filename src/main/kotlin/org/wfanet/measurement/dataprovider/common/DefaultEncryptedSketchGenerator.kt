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

package org.wfanet.measurement.dataprovider.common

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SketchConfig

/** [EncryptedSketchGenerator] that delegates to helpers. */
class DefaultEncryptedSketchGenerator(
  private val elGamalPublicKeyStore: ElGamalPublicKeyStore,
  private val sketchConfigStore: SketchConfigStore,
  private val generateSketch: (RequisitionSpec, ElGamalPublicKey, SketchConfig) -> Flow<ByteString>
) : EncryptedSketchGenerator {
  override suspend fun generate(
    requisitionSpec: RequisitionSpec,
    encryptedSketch: MeasurementSpec.EncryptedSketch
  ): Flow<ByteString> {
    val publicKey = elGamalPublicKeyStore.get(encryptedSketch.combinedPublicKey)
    val sketchConfig = sketchConfigStore.get(encryptedSketch.sketchConfig)
    return generateSketch(requisitionSpec, publicKey, sketchConfig)
  }
}
