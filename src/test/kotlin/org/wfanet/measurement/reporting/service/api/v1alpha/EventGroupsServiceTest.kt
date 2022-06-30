package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.Any
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.Duration
import com.google.protobuf.kotlin.toByteString
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.loadPrivateKey
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.loadPublicKey
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest

private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
val ENCRYPTION_PRIVATE_KEY = loadEncryptionPrivateKey("mc_enc_private.tink")
val ENCRYPTION_PUBLIC_KEY = loadEncryptionPublicKey("mc_enc_public.tink")
private val TEST_MESSAGE = testMetadataMessage {
  name = "Bob"
  value = 1
  duration = Duration.newBuilder().setSeconds(30).build()
}
private val EVENT_GROUP = eventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
  encryptedMetadata = ENCRYPTION_PUBLIC_KEY.hybridEncrypt(metadata {
    eventGroupMetadataDescriptor = METADATA_NAME
    metadata = Any.pack(TEST_MESSAGE)
  }.toByteString())
}
private const val MEASUREMENT_CONSUMER_EXTERNAL_ID = 111L
private val MEASUREMENT_CONSUMER_REFERENCE_ID = externalIdToApiId(MEASUREMENT_CONSUMER_EXTERNAL_ID)
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID).toName()
private const val DATA_PROVIDER_NAME = "dataProviders/123"
private const val METADATA_NAME = "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
  name = METADATA_NAME
  descriptorSet = TEST_MESSAGE.descriptorForType.getFileDescriptorSet()
}

@RunWith(JUnit4::class)
class EventGroupsServiceTest {
  private val cmmsEventGroupsServiceMock: EventGroupsCoroutineImplBase =
    mockService() {
      onBlocking { listEventGroups(any()) }
        .thenReturn(listEventGroupsResponse { eventGroups += EVENT_GROUP })
    }
  private val cmmsEventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService() {
      onBlocking { batchGetEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          batchGetEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupsServiceMock)
    addService(cmmsEventGroupMetadataDescriptorsServiceMock)
  }

  @Test
  fun `EventGroupsService returns list`() {
    val encrypted = metadata {
      eventGroupMetadataDescriptor = METADATA_NAME
      metadata = Any.pack(TEST_MESSAGE)
    }
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_PRIVATE_KEY
      )
    val result = withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking {
          eventGroupsService.listEventGroups(listEventGroupsRequest { parent = DATA_PROVIDER_NAME })
      }
    }
  }
}

fun Descriptor.getFileDescriptorSet(): FileDescriptorSet {
  val fileDescriptors = mutableSetOf<FileDescriptor>()
  val toVisit = mutableListOf<FileDescriptor>(file)
  while (toVisit.isNotEmpty()) {
    val fileDescriptor = toVisit.removeLast()
    if (!fileDescriptors.contains(fileDescriptor)) {
      fileDescriptors.add(fileDescriptor)
      fileDescriptor.dependencies.forEach {
        if (!fileDescriptors.contains(it)) {
          toVisit.add(it)
        }
      }
    }
  }
  return FileDescriptorSet.newBuilder().addAllFile(fileDescriptors.map { it.toProto() }).build()
}

fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
  return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

fun loadEncryptionPublicKey(fileName: String): TinkPublicKeyHandle {
  return loadPublicKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}
