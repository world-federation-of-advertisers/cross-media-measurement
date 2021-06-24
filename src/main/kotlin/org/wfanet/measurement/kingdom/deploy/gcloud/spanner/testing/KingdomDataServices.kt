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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerMeasurementConsumersService
import org.wfanet.measurement.kingdom.service.internal.testing.KingdomDataServices

fun makeSpannerKingdomDataServices(
  clock: Clock,
  idGenerator: IdGenerator,
  databaseClient: AsyncDatabaseClient
): KingdomDataServices {
  return KingdomDataServices(SpannerMeasurementConsumersService(clock, idGenerator, databaseClient))
}
