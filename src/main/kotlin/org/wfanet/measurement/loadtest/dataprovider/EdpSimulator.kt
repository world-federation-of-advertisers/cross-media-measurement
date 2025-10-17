/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import java.security.cert.X509Certificate
import java.time.ZoneId
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.Dispatchers
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeFulfillRequisitionRequestBuilder

class EdpSimulator(
  edpData: DataProviderData,
  edpDisplayName: String,
  measurementConsumerName: String,
  certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub,
  modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  requisitionFulfillmentStubsByDuchyId:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  syntheticDataTimeZone: ZoneId,
  override val eventGroupsOptions: Collection<EventGroupOptions>,
  eventQuery: EventQuery<Message>,
  throttler: Throttler,
  privacyBudgetManager: PrivacyBudgetManager,
  trustedCertificates: Map<ByteString, X509Certificate>,
  vidIndexMap: VidIndexMap? = null,
  sketchEncrypter: SketchEncrypter = SketchEncrypter.Default,
  random: Random = Random,
  logSketchDetails: Boolean = false,
  health: SettableHealth = SettableHealth(),
  blockingCoroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  trusTeeEncryptionParams: TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams? = null,
) :
  AbstractEdpSimulator(
    edpData,
    edpDisplayName,
    measurementConsumerName,
    certificatesStub,
    modelLinesStub,
    dataProvidersStub,
    eventGroupsStub,
    requisitionsStub,
    requisitionFulfillmentStubsByDuchyId,
    syntheticDataTimeZone,
    eventGroupsOptions,
    eventQuery,
    throttler,
    privacyBudgetManager,
    trustedCertificates,
    vidIndexMap,
    sketchEncrypter,
    random,
    logSketchDetails,
    health,
    blockingCoroutineContext,
    trusTeeEncryptionParams,
  ) {

  interface EventGroupOptions : AbstractEdpSimulator.EventGroupOptions {
    val mediaTypes: Set<MediaType>
    val metadata: EventGroupMetadata?
  }

  override suspend fun ensureEventGroups(): List<EventGroup> {
    return eventGroupsOptions.map { ensureEventGroup(it) }
  }

  private suspend fun ensureEventGroup(eventGroupOptions: EventGroupOptions): EventGroup {
    val eventGroupReferenceId = eventGroupReferenceIdPrefix + eventGroupOptions.referenceIdSuffix
    val metadata: EventGroupMetadata? = eventGroupOptions.metadata
    return ensureEventGroup(eventGroupReferenceId) {
      mediaTypes += eventGroupOptions.mediaTypes
      dataAvailabilityInterval =
        eventGroupOptions.getDataAvailabilityInterval(syntheticDataTimeZone)
      if (metadata == null) {
        clearEventGroupMetadata()
      } else {
        eventGroupMetadata = metadata
      }
    }
  }
}
