package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import io.grpc.Channel
import java.io.File
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerApp
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageConfigType
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

class ResultsFulfillerTestApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  cmmsChannel: Channel,
  private val fileSystemRootDirectory: File,
  override val kmsClient: FakeKmsClient,
  private val principalName: String,
) :
  ResultsFulfillerApp(
    subscriptionId,
    queueSubscriber,
    parser,
    workItemsClient,
    workItemAttemptsClient,
    overrideRequisitionsStub =
      RequisitionsCoroutineStub(cmmsChannel).withPrincipalName(principalName),
  ) {
  override fun createStorageClient(
    blobUri: String,
    rootDirectory: File?,
    projectId: String?,
  ): StorageClient {
    return SelectedStorageClient(blobUri, rootDirectory, projectId)
  }

  override val typeRegistry: TypeRegistry
    get() = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

  override fun getStorageConfig(
    configType: StorageConfigType,
    storageParams: ResultsFulfillerParams.StorageParams,
  ): StorageConfig {
    return StorageConfig(rootDirectory = fileSystemRootDirectory)
  }
}
