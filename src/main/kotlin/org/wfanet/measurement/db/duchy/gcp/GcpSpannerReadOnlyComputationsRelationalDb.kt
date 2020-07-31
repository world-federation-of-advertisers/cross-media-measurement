// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.spanner.DatabaseClient
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toCollection
import org.wfanet.measurement.db.duchy.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken

/**
 * Implementation of [ReadOnlyComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerReadOnlyComputationsRelationalDb(
  private val databaseClient: DatabaseClient,
  private val parseLongFunc: (Long) -> ComputationStage,
  private val convertToLong: (ComputationStage) -> Long
) : ReadOnlyComputationsRelationalDb {

  override suspend fun readComputationToken(globalId: Long): ComputationToken =
    ComputationTokenProtoQuery(parseLongFunc, globalId)
      .execute(databaseClient)
      .single()

  override suspend fun readGlobalComputationIds(stages: Set<ComputationStage>): Set<Long> =
    GlobalIdsQuery(convertToLong, stages)
      .execute(databaseClient)
      .toCollection(mutableSetOf())
}
