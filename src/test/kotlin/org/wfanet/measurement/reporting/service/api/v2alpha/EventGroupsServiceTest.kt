/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.eventGroup
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest

@RunWith(JUnit4::class)
class EventGroupsServiceTest {
  private val publicKingdomEventGroupsMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any()) }
      .thenReturn(
        listEventGroupsResponse {
          eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2)
          nextPageToken = ""
        }
      )
  }

  private val publicKingdomEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(publicKingdomEventGroupsMock)
    addService(publicKingdomEventGroupMetadataDescriptorsMock)
  }

  private lateinit var service: EventGroupsService

  @Before
  fun initService() {
    val celEnvCacheProvider =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        EventGroup.getDescriptor(),
        Duration.ofSeconds(5),
        Dispatchers.Default,
      )

    service =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE,
        celEnvCacheProvider
      )
  }

  @Test
  fun `listEventGroups returns all event groups as is when no filter`() = runBlocking {
    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups returns only event groups that match filter when filter has metadata`() {
    val testMessage = testMetadataMessage { publisherId = 5 }

    val cmmsEventGroup2 =
      CMMS_EVENT_GROUP.copy {
        encryptedMetadata =
          encryptMetadata(
            CmmsEventGroupKt.metadata {
              eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              metadata = Any.pack(testMessage)
            },
            ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
          )
      }

    runBlocking {
      whenever(publicKingdomEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          listEventGroupsResponse { eventGroups += listOf(CMMS_EVENT_GROUP, cmmsEventGroup2) }
        )
    }

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "metadata.metadata.publisher_id > 5"
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups returns only event groups that match filter when filter has no metadata`() {
    val cmmsEventGroup2 =
      CMMS_EVENT_GROUP.copy { eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID + 2 }

    runBlocking {
      whenever(publicKingdomEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          listEventGroupsResponse { eventGroups += listOf(CMMS_EVENT_GROUP, cmmsEventGroup2) }
        )
    }

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "event_group_reference_id == \"$EVENT_GROUP_REFERENCE_ID\""
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP)

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups uses page token when page token present`() {
    val pageToken =
      listEventGroupsPageToken { externalMeasurementConsumerId = 1234 }
        .toByteString()
        .base64UrlEncode()

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              this.pageToken = pageToken
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
          this.pageToken = pageToken
        }
      )
  }

  @Test
  fun `listEventGroups returns nextPageToken when it is present`() {
    val nextPageToken =
      listEventGroupsPageToken { externalMeasurementConsumerId = 1234 }
        .toByteString()
        .base64UrlEncode()

    runBlocking {
      whenever(publicKingdomEventGroupsMock.listEventGroups(any()))
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2)
            this.nextPageToken = nextPageToken
          }
        )
    }

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()
    assertThat(response.nextPageToken).isEqualTo(nextPageToken)

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups use default page_size when page_size is too small`() {
    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              pageSize = 0
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = DEFAULT_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups use max page_size when page_size is too big`() {
    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              pageSize = MAX_PAGE_SIZE + 1
            }
          )
        }
      }

    assertThat(response.eventGroupsList).containsExactly(EVENT_GROUP, EVENT_GROUP_2).inOrder()

    verifyProtoArgument(publicKingdomEventGroupsMock, EventGroupsCoroutineImplBase::listEventGroups)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        cmmsListEventGroupsRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          pageSize = MAX_PAGE_SIZE
        }
      )
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED when principal isn't reporting principal`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.message).contains("No ReportingPrincipal")
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.message).contains("No ReportingPrincipal")
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when principal doesn't match parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME + 2 }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("MeasurementConsumer")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when page_size is negative`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                pageSize = -1
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("page_size")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listEventGroups(listEventGroupsRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when parent is malformed`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = "$MEASUREMENT_CONSUMER_NAME//"
                pageSize = -1
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when operator overload in filter doesn't exist`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name > 5"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when operator in filter doesn't exist`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name >>> 5"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when filter eval doesn't result in boolean`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                filter = "name"
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("boolean")
  }

  @Test
  fun `listEventGroups throws FAILED_PRECONDITION when store doesn't have private key`() {
    val celEnvCacheProvider =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        EventGroup.getDescriptor(),
        Duration.ofSeconds(5),
        Dispatchers.Default,
      )

    service =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        InMemoryEncryptionKeyPairStore(mapOf()),
        celEnvCacheProvider
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            service.listEventGroups(listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("private key")
  }

  @Test
  fun `listEventGroups throws RUNTIME_EXCEPTION when event group doesn't have filter field`() {
    assertFailsWith<RuntimeException> {
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          service.listEventGroups(
            listEventGroupsRequest {
              parent = MEASUREMENT_CONSUMER_NAME
              filter = "field_that_doesnt_exist = 10"
            }
          )
        }
      }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }
    private const val MEASUREMENT_CONSUMER_ID = "1234"
    private val MEASUREMENT_CONSUMER_NAME = MeasurementConsumerKey(MEASUREMENT_CONSUMER_ID).toName()

    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private val ENCRYPTION_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
    private val ENCRYPTION_KEY_PAIR_STORE =
      InMemoryEncryptionKeyPairStore(
        mapOf(
          MEASUREMENT_CONSUMER_NAME to
            listOf(ENCRYPTION_PUBLIC_KEY.toByteString() to ENCRYPTION_PRIVATE_KEY)
        )
      )

    private const val DATA_PROVIDER_ID = "1235"
    private val DATA_PROVIDER_NAME = DataProviderKey(DATA_PROVIDER_ID).toName()

    private val TEST_MESSAGE = testMetadataMessage { publisherId = 15 }
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      EventGroupMetadataDescriptorKey(DATA_PROVIDER_ID, "1236").toName()
    private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
      descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
    }

    private const val EVENT_GROUP_REFERENCE_ID = "ref"
    private const val EVENT_GROUP_ID = "1237"
    private val CMMS_EVENT_GROUP_NAME = CmmsEventGroupKey(DATA_PROVIDER_ID, EVENT_GROUP_ID).toName()
    private val CMMS_EVENT_GROUP = cmmsEventGroup {
      name = CMMS_EVENT_GROUP_NAME
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      measurementConsumerPublicKey = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().pack()
      eventTemplates += CmmsEventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
        )
      state = CmmsEventGroup.State.ACTIVE
    }

    private const val EVENT_GROUP_REFERENCE_ID_2 = "ref2"
    private const val EVENT_GROUP_ID_2 = "2237"
    private val CMMS_EVENT_GROUP_NAME_2 =
      CmmsEventGroupKey(DATA_PROVIDER_ID, EVENT_GROUP_ID_2).toName()
    private val CMMS_EVENT_GROUP_2 = cmmsEventGroup {
      name = CMMS_EVENT_GROUP_NAME_2
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
      measurementConsumerPublicKey = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().pack()
      eventTemplates += CmmsEventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
        )
      state = CmmsEventGroup.State.ACTIVE
    }

    private val EVENT_GROUP = eventGroup {
      name = EventGroupKey(MEASUREMENT_CONSUMER_ID, EVENT_GROUP_ID).toName()
      cmmsEventGroup = CMMS_EVENT_GROUP_NAME
      cmmsDataProvider = DATA_PROVIDER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      eventTemplates += EventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      metadata =
        EventGroupKt.metadata {
          eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
          metadata = Any.pack(TEST_MESSAGE)
        }
    }

    private val EVENT_GROUP_2 = eventGroup {
      name = EventGroupKey(MEASUREMENT_CONSUMER_ID, EVENT_GROUP_ID_2).toName()
      cmmsEventGroup = CMMS_EVENT_GROUP_NAME_2
      cmmsDataProvider = DATA_PROVIDER_NAME
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
      eventTemplates += EventGroupKt.eventTemplate { type = TestEvent.getDescriptor().fullName }
      metadata =
        EventGroupKt.metadata {
          eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
          metadata = Any.pack(TEST_MESSAGE)
        }
    }
  }
}
