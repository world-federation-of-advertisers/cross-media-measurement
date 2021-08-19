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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.EncryptedSketch
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * Interface for handling Requisitions with an [EncryptedSketch]-typed MeasurementSpec.
 */
interface EncryptedSketchGenerator {
  /**
   * Generates an encrypted sketch as a flow of ByteStrings.
   *
   * Each ByteString's size should be optimized for gRPC network transmission (several KiB).
   */
  suspend fun generate(
    requisitionSpec: RequisitionSpec,
    encryptedSketch: EncryptedSketch
  ): Flow<ByteString>
}
