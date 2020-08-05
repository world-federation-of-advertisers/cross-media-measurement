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

package org.wfanet.measurement.service.internal.kingdom

import io.grpc.BindableService
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase

/** Builds a list of all the Kingdom's internal storage services. */
fun buildStorageServices(relationalDatabase: KingdomRelationalDatabase): List<BindableService> =
  listOf(
    ReportConfigScheduleStorageService(relationalDatabase),
    ReportConfigStorageService(relationalDatabase),
    ReportStorageService(relationalDatabase),
    ReportLogEntryStorageService(relationalDatabase),
    RequisitionStorageService(relationalDatabase)
  )

/**
 * Makes a [CommonServer] on [port] exporting all of the internal Kingdom storage-layer services.
 */
fun buildKingdomStorageServer(
  relationalDatabase: KingdomRelationalDatabase,
  port: Int,
  nameForLogging: String = "KingdomStorageServer"
): CommonServer =
  CommonServer(
    nameForLogging,
    port,
    *buildStorageServices(relationalDatabase).toTypedArray()
  )
