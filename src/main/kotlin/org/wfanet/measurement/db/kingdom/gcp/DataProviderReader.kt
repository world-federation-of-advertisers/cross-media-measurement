package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.DataProvider

class DataProviderReader : SpannerReader<DataProviderReader.Result>() {
  data class Result(
    val dataProvider: DataProvider,
    val dataProviderId: Long
  )

  override val baseSql: String =
    """
    SELECT
      DataProviders.DataProviderId,
      DataProviders.ExternalDataProviderId,
      DataProviders.DataProviderDetails,
      DataProviders.DataProviderDetailsJson
    FROM DataProviders
    """.trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildDataProvider(struct), struct.getLong("DataProviderId"))

  private fun buildDataProvider(struct: Struct): DataProvider = DataProvider.newBuilder().apply {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
  }.build()

  companion object {
    /** Returns a [Result] given an external advertiser id, or null if no such id exists. */
    suspend fun forExternalId(
      readContext: ReadContext,
      externalDataProviderId: ExternalId
    ): Result? {
      return DataProviderReader()
        .withBuilder {
          appendClause("WHERE DataProviders.ExternalDataProviderId = @external_data_provider_id")
          bind("external_data_provider_id").to(externalDataProviderId.value)
        }
        .execute(readContext)
        .singleOrNull()
    }
  }
}
