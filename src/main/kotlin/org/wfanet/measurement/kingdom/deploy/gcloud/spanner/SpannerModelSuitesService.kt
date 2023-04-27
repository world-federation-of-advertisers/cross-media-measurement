package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetModelSuiteRequest
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelSuites
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelSuite

class SpannerModelSuitesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase() {

  override suspend fun createModelSuite(request: ModelSuite): ModelSuite {
    grpcRequire(request.displayName.isNotEmpty()) {
      "DisplayName field of ModelSuite is missing fields."
    }
    return CreateModelSuite(request).execute(client, idGenerator)
  }

  override suspend fun getModelSuite(request: GetModelSuiteRequest): ModelSuite {
    return ModelSuiteReader()
      .readByExternalModelSuiteId(client.singleUse(), ExternalId(request.externalModelSuiteId))
      ?.modelSuite
      ?: failGrpc(Status.NOT_FOUND) { "ModelSuite not found" }
  }

  override fun streamModelSuites(request: StreamModelSuitesRequest): Flow<ModelSuite> {
    return StreamModelSuites(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelSuite
    }
  }
}
