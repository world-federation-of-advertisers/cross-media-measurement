// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Measurement

class MeasurementDetailsReader() : SpannerReader<MeasurementDetailsReader.Result>() {

  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val measurementDetails: Measurement.Details
  )

  override val baseSql =
    """
    SELECT
      Measurements.MeasurementId,
      Measurements.MeasurementConsumerId,
      Measurements.MeasurementDetails,
    FROM
      Measurements
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      struct.getInternalId("MeasurementConsumerId"),
      struct.getInternalId("MeasurementId"),
      struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
    )
}
