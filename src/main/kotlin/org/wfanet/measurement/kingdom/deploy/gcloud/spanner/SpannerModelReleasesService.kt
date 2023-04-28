package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelRelease

class SpannerModelReleasesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelReleasesCoroutineImplBase() {

  override suspend fun createModelRelease(request: ModelRelease): ModelRelease {
    return CreateModelRelease(request).execute(client, idGenerator)
  }
}
