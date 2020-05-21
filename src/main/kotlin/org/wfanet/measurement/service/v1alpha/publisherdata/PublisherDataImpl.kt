package org.wfanet.measurement.service.v1alpha.publisherdata

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt

class PublisherDataImpl : PublisherDataGrpcKt.PublisherDataCoroutineImplBase() {
  override suspend fun getCombinedPublicKey(
    request: GetCombinedPublicKeyRequest
  ): CombinedPublicKey {
    if (request.key.combinedPublicKeyId.isEmpty()) {
      throw StatusRuntimeException(Status.INVALID_ARGUMENT)
    } else {
      return CombinedPublicKey.newBuilder()
        .setKey(request.key)
        .build()
    }
  }
}
