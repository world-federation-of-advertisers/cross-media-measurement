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
import kotlin.reflect.full.declaredMemberProperties
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase

interface DataServices {

  /** Builds a list of all the Kingdom's internal data-layer services. */
  fun buildDataServices(): KingdomDataServices
}

data class KingdomDataServices(
  val accountsService: AccountsCoroutineImplBase,
  val apiKeysService: ApiKeysGrpcKt.ApiKeysCoroutineImplBase,
  val certificatesService: CertificatesCoroutineImplBase,
  val dataProvidersService: DataProvidersCoroutineImplBase,
  val modelProvidersService: ModelProvidersCoroutineImplBase,
  val eventGroupMetadataDescriptorsService: EventGroupMetadataDescriptorsCoroutineImplBase,
  val eventGroupsService: EventGroupsCoroutineImplBase,
  val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  val measurementsService: MeasurementsCoroutineImplBase,
  val publicKeysService: PublicKeysCoroutineImplBase,
  val requisitionsService: RequisitionsCoroutineImplBase,
  val computationParticipantsService: ComputationParticipantsCoroutineImplBase,
  val measurementLogEntriesService: MeasurementLogEntriesCoroutineImplBase,
  val recurringExchangesService: RecurringExchangesCoroutineImplBase,
  val exchangesService: ExchangesCoroutineImplBase,
  val exchangeStepsService: ExchangeStepsCoroutineImplBase,
  val exchangeStepAttemptsService: ExchangeStepAttemptsCoroutineImplBase
)

fun KingdomDataServices.toList(): List<BindableService> {
  return KingdomDataServices::class.declaredMemberProperties.map { it.get(this) as BindableService }
}
