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

package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyRequest
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyResponse

class SpannerMeasurementsService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : MeasurementsCoroutineImplBase() {
  override suspend fun createDataProvider(request: DataProvider): DataProvider {
    TODO("not implemented yet")
  }
  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    TODO("not implemented yet")
  }
  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    TODO("not implemented yet")
  }
  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    TODO("not implemented yet")
  }
  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    TODO("not implemented yet")
  }

  override suspend fun createCertificate(request: Certificate): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    TODO("not implemented yet")
  }
  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    TODO("not implemented yet")
  }

  override suspend fun updatePublicKey(request: UpdatePublicKeyRequest): UpdatePublicKeyResponse {
    TODO("not implemented yet")
  }

  override suspend fun createMeasurement(request: Measurement): Measurement {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    TODO("not implemented yet")
  }
  override suspend fun getMeasurementByComputationId(
    request: GetMeasurementByComputationIdRequest
  ): Measurement {
    TODO("not implemented yet")
  }

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun getRequisitionByDataProviderId(
    request: GetRequisitionByDataProviderIdRequest
  ): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
}
