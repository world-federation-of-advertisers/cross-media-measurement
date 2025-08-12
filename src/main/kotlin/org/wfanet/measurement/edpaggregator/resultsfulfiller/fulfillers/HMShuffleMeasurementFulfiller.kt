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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers

import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.shareshuffle.FulfillRequisitionRequestBuilder

class HMShuffleMeasurementFulfiller(
  private val requisition: Requisition,
  private val requisitionNonce: Long,
  private val sampledFrequencyVector: FrequencyVector,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub,
) : MeasurementFulfiller {
  override suspend fun fulfillRequisition() {
    logger.info("Fulfilling requisition ${requisition.name}...")
    val requests: Flow<FulfillRequisitionRequest> =
      FulfillRequisitionRequestBuilder.build(
          requisition,
          requisitionNonce,
          sampledFrequencyVector,
          dataProviderCertificateKey,
          dataProviderSigningKeyHandle,
        )
        .asFlow()
    try {
      requisitionFulfillmentStub.fulfillRequisition(requests)
    } catch (e: StatusException) {
      throw Exception("Error fulfilling requisition ${requisition.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
