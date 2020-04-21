package org.wfanet.measurement.service.v1alpha.publisherdata

import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpc
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey

class PublisherDataImpl : PublisherDataGrpc.PublisherDataImplBase() {
  override fun getCombinedPublicKey(
    req: GetCombinedPublicKeyRequest,
    responseObserver: StreamObserver<CombinedPublicKey>
  ) {
    responseObserver.onError(StatusRuntimeException(Status.UNIMPLEMENTED))
  }
}