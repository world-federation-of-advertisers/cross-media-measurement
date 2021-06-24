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

package org.wfanet.measurement.kingdom.deploy.common.service

import io.grpc.BindableService
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
<<<<<<< HEAD
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
=======
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
>>>>>>> 1f08a28c (addressed comments)
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase

interface DataServices {

  /** Builds a list of all the Kingdom's internal data-layer services. */
  fun buildDataServices(): KingdomDataServices
}

data class KingdomDataServices(
  val certificatesService: CertificatesCoroutineImplBase,
  val dataProvidersService: DataProvidersCoroutineImplBase,
  val eventGroupsService: EventGroupsCoroutineImplBase,
  val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  val measurementsService: MeasurementsCoroutineImplBase,
<<<<<<< HEAD
  val requisitionsService: RequisitionsCoroutineImplBase,
  val computationParticipantsService: ComputationParticipantsCoroutineImplBase,
  val measurementLogEntriesService: MeasurementLogEntriesCoroutineImplBase
=======
  val requisitionsService: RequisitionsCoroutineImplBase
>>>>>>> 1f08a28c (addressed comments)
)

fun KingdomDataServices.toList(): List<BindableService> {
  return listOf(
    certificatesService,
    dataProvidersService,
    eventGroupsService,
    measurementConsumersService,
    measurementsService,
<<<<<<< HEAD
    requisitionsService,
    computationParticipantsService,
    measurementLogEntriesService
=======
    requisitionsService
>>>>>>> 1f08a28c (addressed comments)
  )
}
