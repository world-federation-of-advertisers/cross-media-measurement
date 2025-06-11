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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyRequest
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyResponse
import org.wfanet.measurement.internal.kingdom.updatePublicKeyResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdatePublicKey

class SpannerPublicKeysService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PublicKeysGrpcKt.PublicKeysCoroutineImplBase(coroutineContext) {

  override suspend fun updatePublicKey(request: UpdatePublicKeyRequest): UpdatePublicKeyResponse {
    grpcRequire(
      request.externalDataProviderId != 0L || request.externalMeasurementConsumerId != 0L
    ) {
      "Parent Id is specified"
    }

    grpcRequire(request.externalCertificateId != 0L) { "Certificate Id unspecified" }

    grpcRequire(request.apiVersion.isNotBlank()) { "API version unspecified" }

    grpcRequire(!request.publicKey.isEmpty) { "Public key unspecified" }
    grpcRequire(!request.publicKeySignature.isEmpty) { "Public key signature unspecified" }

    try {
      UpdatePublicKey(request).execute(client, idGenerator)
      return updatePublicKeyResponse {}
    } catch (e: CertificateNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate not found.")
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }
}
