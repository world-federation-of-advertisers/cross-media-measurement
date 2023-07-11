package org.wfanet.measurement.integration.gcloud

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import kotlinx.coroutines.debug.junit4.CoroutinesTimeout
import org.junit.Rule
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessLifeOfAMeasurementIntegrationTest
import org.wfanet.measurement.integration.common.duchy.PostgresDuchyDependencyProviderRule
import org.wfanet.measurement.storage.StorageClient


/**
 * Implementation of [InProcessLifeOfAMeasurementIntegrationTest] for GCloud backends with Postgres
 * database.
 */
class GCloudPostgresInProcessLifeOfAMeasurementIntegrationTest:
  InProcessLifeOfAMeasurementIntegrationTest() {

  @get:Rule
  val timeout = CoroutinesTimeout.seconds(90)

  override val kingdomDataServicesRule by lazy { KingdomDataServicesProviderRule() }
  override val duchyDependenciesRule by lazy { PostgresDuchyDependencyProviderRule(ALL_DUCHY_NAMES) }
  override val storageClient: StorageClient by lazy {
    GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-simulator")
  }
}
