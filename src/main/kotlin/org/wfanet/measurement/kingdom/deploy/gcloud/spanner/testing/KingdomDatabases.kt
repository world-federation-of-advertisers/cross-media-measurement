package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.db.testing.Databases
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerLegacySchedulingDatabase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerReportDatabase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerRequisitionDatabase

fun makeSpannerKingdomDatabases(
  clock: Clock,
  idGenerator: IdGenerator,
  databaseClient: AsyncDatabaseClient
): Databases {
  return Databases(
    SpannerLegacySchedulingDatabase(clock, idGenerator, databaseClient),
    SpannerReportDatabase(clock, idGenerator, databaseClient),
    SpannerRequisitionDatabase(clock, idGenerator, databaseClient),
    SpannerDatabaseTestHelper(clock, idGenerator, databaseClient)
  )
}
