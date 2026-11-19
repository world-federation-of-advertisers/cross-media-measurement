// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import java.io.File
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionWorkItemDispatcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ContinuousGaussianNoiseSelector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.DefaultFulfillerSelector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.FulfillerSelector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ImpressionDataSourceProvider
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.resultsfulfiller.PipelineConfiguration
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StripedByteFrequencyVector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers.MeasurementFulfiller
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/** Shared real requisition fetching and fulfillment for integration tests. */
object ResultsFulfillerTestHelper {
  /** Executes real requisition fetching and fulfillment against the caller's active output. */
  suspend fun fulfillTestRequisitions(
    provider: DataProviderData,
    kingdomChannel: io.grpc.Channel,
    requisitionMetadata: RequisitionMetadataServiceCoroutineStub,
    sourceProvider: ImpressionDataSourceProvider,
    modelLineInfos: Map<String, ModelLineInfo>,
    kmsClient: com.google.crypto.tink.KmsClient,
    groupedDirectory: File,
    clock: java.time.Clock,
    cutovers: List<ModelLineCutover> = emptyList(),
    aliases: Map<String, String> = emptyMap(),
    onSelectedSource: (String) -> Unit = {},
    onFrequencyVector:
      suspend (MeasurementSpec, RequisitionSpec, StripedByteFrequencyVector) -> Unit,
  ): Int {
    val noThrottle =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T = block()
      }
    val requisitions = RequisitionsCoroutineStub(kingdomChannel).withPrincipalName(provider.name)
    val metadata = requisitionMetadata
    val validator =
      RequisitionsValidator(provider.privateEncryptionKey, allowMixedEventGroupSelectors = false)
    val requisitionsDirectory = groupedDirectory
    val groupedStorage = FileSystemStorageClient(requisitionsDirectory)
    val groupedBlobs = mutableListOf<String>()
    val dispatcher =
      object : RequisitionWorkItemDispatcher {
        override fun workItemName(groupId: String) = "workItems/fulfillment-$groupId"

        override suspend fun dispatch(groupId: String, blobUri: String) {
          groupedBlobs += blobUri
        }

        override suspend fun fail(workItemName: String) {}
      }
    RequisitionFetcher(
        requisitionsStub = requisitions,
        requisitionMetadataStub = metadata,
        storageClient = groupedStorage,
        dataProviderName = provider.name,
        directStoragePathPrefix = "groups",
        blobUriPrefix = "file://" + requisitionsDirectory.absolutePath,
        requisitionValidator = validator,
        requisitionGrouper =
          RequisitionGrouperByReportId(
            validator,
            requisitions,
            EventGroupsCoroutineStub(kingdomChannel).withPrincipalName(provider.name),
            noThrottle,
            noThrottle,
          ),
        metadataThrottler = noThrottle,
        kingdomListThrottler = noThrottle,
        workItemDispatcher = dispatcher,
        requisitionRefusalDuration = Duration.ofDays(2),
        clock = clock,
        modelLineCutovers = cutovers,
        legacyModelLineMap = aliases,
      )
      .fetchAndStoreRequisitions()
    check(groupedBlobs.isNotEmpty()) { "No requisitions were fetched" }
    val delegate =
      DefaultFulfillerSelector(
        requisitions,
        noThrottle,
        noThrottle,
        emptyMap(),
        provider.certificateKey,
        provider.signingKeyHandle,
        ContinuousGaussianNoiseSelector(),
        null,
        false,
        null,
        supportedMultiPartyNoiseMechanisms = emptySet(),
        trusTeeV2ImpressionCountMode =
          ResultsFulfillerParams.TrusTeeV2Config.ImpressionCountMode.UNSPECIFIED,
        trusTeeV2MaxFrequencyPerUser = null,
      )
    for (uri in groupedBlobs) {
      val group =
        Any.parseFrom(
            checkNotNull(groupedStorage.getBlob("groups/" + uri.substringAfterLast('/')))
              .read()
              .flatten()
          )
          .unpack(GroupedRequisitions::class.java)
      onSelectedSource(group.selectedModelLine)
      val selection =
        object : FulfillerSelector {
          override suspend fun selectFulfiller(
            requisition: Requisition,
            measurementSpec: MeasurementSpec,
            requisitionSpec: RequisitionSpec,
            frequencyVector: StripedByteFrequencyVector,
            populationSpec: PopulationSpec,
            kekUri: String?,
          ): MeasurementFulfiller {
            onFrequencyVector(measurementSpec, requisitionSpec, frequencyVector)
            return delegate.selectFulfiller(
              requisition,
              measurementSpec,
              requisitionSpec,
              frequencyVector,
              populationSpec,
              kekUri,
            )
          }
        }
      ResultsFulfiller(
          provider.name,
          metadata,
          requisitions,
          noThrottle,
          noThrottle,
          provider.privateEncryptionKey,
          group,
          modelLineInfos,
          PipelineConfiguration(16, 4, 2, 2, 2),
          sourceProvider,
          kmsClient,
          StorageConfig(rootDirectory = File("/")),
          selection,
          emptySet(),
          metrics = ResultsFulfillerMetrics(Instrumentation.meter),
        )
        .fulfillRequisitions()
      for (entry in group.requisitionsList) {
        val requisition = entry.requisition.unpack(Requisition::class.java)
        val actual = requisitions.getRequisition(getRequisitionRequest { name = requisition.name })
        assertThat(actual.state).isEqualTo(Requisition.State.FULFILLED)
      }
    }
    return groupedBlobs.size
  }
}
