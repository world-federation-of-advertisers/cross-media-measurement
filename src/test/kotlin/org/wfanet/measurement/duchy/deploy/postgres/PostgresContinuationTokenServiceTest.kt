package org.wfanet.measurement.duchy.deploy.postgres

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.duchy.deploy.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH
import org.wfanet.measurement.duchy.service.internal.testing.ContinuationTokensServiceTest
import java.time.Clock
import kotlin.random.Random

@RunWith(JUnit4::class)
class PostgresContinuationTokenServiceTest : ContinuationTokensServiceTest<PostgresContinuationTokenService>() {
  override fun newService(): PostgresContinuationTokenService {
    val client = EmbeddedPostgresDatabaseProvider(DUCHY_CHANGELOG_PATH).createNewDatabase()
    val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))
    return PostgresContinuationTokenService(client, idGenerator)
  }
}
