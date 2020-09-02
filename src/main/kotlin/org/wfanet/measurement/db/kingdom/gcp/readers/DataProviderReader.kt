package org.wfanet.measurement.db.kingdom.gcp.readers

import com.google.cloud.spanner.Struct
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

  override val externalIdColumn: String = "DataProviders.ExternalDataProviderId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildDataProvider(struct), struct.getLong("DataProviderId"))

  private fun buildDataProvider(struct: Struct): DataProvider = DataProvider.newBuilder().apply {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
  }.build()
}
