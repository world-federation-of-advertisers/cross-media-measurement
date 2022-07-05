// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.kotlin.toByteString
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.age
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.duration
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.name
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.consent.client.common.signMessage
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.loadPrivateKey
import org.wfanet.measurement.reporting.service.api.v1alpha.crypto.loadPublicKey
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata as reportingMetadata
import org.wfanet.measurement.reporting.v1alpha.eventGroup as reportingEventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse as reportingListEventGroupsResponse

private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
val ENCRYPTION_PRIVATE_KEY = loadEncryptionPrivateKey("mc_enc_private.tink")
val ENCRYPTION_PUBLIC_KEY = loadEncryptionPublicKey("mc_enc_public.tink")
val EDP_SIGNING_KEY = loadSigningKey("edp1_cs_cert.der", "edp1_cs_private.der")
private val TEST_MESSAGE = testMetadataMessage {
  name = name { value = "Bob" }
  age = age { value = 15 }
  duration = duration { value = 20 }
}
private val EVENT_GROUP = eventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
  encryptedMetadata =
    ENCRYPTION_PUBLIC_KEY.hybridEncrypt(
      signMessage(
          metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          EDP_SIGNING_KEY
        )
        .toByteString()
    )
}
private val TEST_MESSAGE_2 = testMetadataMessage {
  name = name { value = "Alice" }
  age = age { value = 5 }
  duration = duration { value = 20 }
}
private val EVENT_GROUP_2 = eventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAGs"
  encryptedMetadata =
    ENCRYPTION_PUBLIC_KEY.hybridEncrypt(
      signMessage(
          metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(TEST_MESSAGE_2)
          },
          EDP_SIGNING_KEY
        )
        .toByteString()
    )
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
        .thenReturn(listEventGroupsResponse { eventGroups += listOf(EVENT_GROUP, EVENT_GROUP_2) })
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
  fun `listEventGroups filters list`() {
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_PRIVATE_KEY
      )
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = DATA_PROVIDER_NAME
              filter = "age.value > 10"
            }
          )
        }
      }

    assertThat(result)
      .isEqualTo(
        reportingListEventGroupsResponse {
          eventGroups += reportingEventGroup {
            name = EVENT_GROUP.name
            metadata = reportingMetadata {
              eventGroupMetadataDescriptor = METADATA_NAME
              metadata = Any.pack(TEST_MESSAGE)
            }
          }
        }
      )
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

fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
  return loadSigningKey(
    SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
    SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile()
  )
}
