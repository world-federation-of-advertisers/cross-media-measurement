package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import org.junit.Rule
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.ModelRolloutsServiceTest

class SpannerModelRolloutsServiceTest : ModelRolloutsServiceTest<SpannerModelRolloutsService>() {

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)

  override fun newServices(
    clock: Clock,
    idGenerator: IdGenerator
  ): Services<SpannerModelRolloutsService> {
    val spannerServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient).buildDataServices()

    return Services(
      spannerServices.modelRolloutsService as SpannerModelRolloutsService,
      spannerServices.modelProvidersService,
      spannerServices.modelSuitesService,
      spannerServices.modelLinesService,
      spannerServices.modelReleasesService
    )
  }
}
