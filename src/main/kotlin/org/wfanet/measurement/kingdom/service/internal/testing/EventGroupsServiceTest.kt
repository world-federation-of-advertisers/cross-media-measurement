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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

private const val RANDOM_SEED = 1
private const val EXTERNAL_EVENT_GROUP_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private const val PROVIDED_EVENT_GROUP_ID = "ProvidedEventGroupId"
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_MC_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a MC certificate der.")
private val PREFERRED_DP_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a DP certificate der.")
private val PREFERRED_MC_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a MC subject key identifier.")
private val PREFERRED_DP_SUBJECT_KEY_IDENTIFIER =
  ByteString.copyFromUtf8("This is a DP subject key identifier.")

@RunWith(JUnit4::class)
abstract class EventGroupsServiceTest<T : EventGroupsCoroutineImplBase> {

  protected val idGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))
  protected val copyIdGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))

  protected lateinit var eventGroupsService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): EventGroupAndHelperServices<T>

  private suspend fun insertMeasurementConsumer(): Long {
    val createdMeasurementConsumer =
      measurementConsumersService.createMeasurementConsumer(
        MeasurementConsumer.newBuilder()
          .apply {
            preferredCertificateBuilder.apply {
              notValidBeforeBuilder.seconds = 12345
              notValidAfterBuilder.seconds = 23456
              subjectKeyIdentifier = PREFERRED_MC_SUBJECT_KEY_IDENTIFIER
              detailsBuilder.setX509Der(PREFERRED_MC_CERTIFICATE_DER)
            }
            detailsBuilder.apply {
              apiVersion = "2"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
            }
          }
          .build()
      )

    // An InternalId for MeasurementConsumer's Certificate is generated.
    copyIdGenerator.generateInternalId()
    // An InternalId for MeasurementConsumer is generated.
    copyIdGenerator.generateInternalId()
    // An External for MeasurementConsumer is generated.
    val externalMeasurementConsumerId = copyIdGenerator.generateExternalId()
    // An External for MeasurementConsumerCertificate is generated.
    copyIdGenerator.generateExternalId()

    // We make sure that the externalMeasurementConsumerId is correct.
    val measurementConsumerRead =
      measurementConsumersService.getMeasurementConsumer(
        GetMeasurementConsumerRequest.newBuilder()
          .setExternalMeasurementConsumerId(externalMeasurementConsumerId.value)
          .build()
      )

    return externalMeasurementConsumerId.value
  }

  private suspend fun insertDataProvider(): Long {
    val createdDataProvider =
      dataProvidersService.createDataProvider(
        DataProvider.newBuilder()
          .apply {
            preferredCertificateBuilder.apply {
              notValidBeforeBuilder.seconds = 12345
              notValidAfterBuilder.seconds = 23456
              subjectKeyIdentifier = PREFERRED_DP_SUBJECT_KEY_IDENTIFIER
              detailsBuilder.setX509Der(PREFERRED_DP_CERTIFICATE_DER)
            }
            detailsBuilder.apply {
              apiVersion = "2"
              publicKey = PUBLIC_KEY
              publicKeySignature = PUBLIC_KEY_SIGNATURE
            }
          }
          .build()
      )

    // An InternalId for DataProvider's Certificate is generated.
    copyIdGenerator.generateInternalId()
    // An InternalId for DataProvider is generated.
    copyIdGenerator.generateInternalId()
    // An External for DataProvider is generated.
    val externalDataProviderId = copyIdGenerator.generateExternalId()
    // An External for DataProviderCertificate is generated.
    copyIdGenerator.generateExternalId()

    // We make sure that the externalDataProviderId is correct.
    val dataProviderRead =
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .setExternalDataProviderId(externalDataProviderId.value)
          .build()
      )

    return externalDataProviderId.value
  }

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    eventGroupsService = services.eventGroupsService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `getEventGroup fails for missing EventGroup`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.getEventGroup(
          GetEventGroupRequest.newBuilder().setExternalEventGroupId(EXTERNAL_EVENT_GROUP_ID).build()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createEventGroup fails for missing data provider`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()

    val eventGroup =
      EventGroup.newBuilder()
        .also {
          it.externalDataProviderId = FIXED_GENERATED_EXTERNAL_ID
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.providedEventGroupId = PROVIDED_EVENT_GROUP_ID
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { eventGroupsService.createEventGroup(eventGroup) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: DataProvider not found")
  }

  @Test
  fun `createEventGroup fails for missing measurement consumer`() = runBlocking {
    val externalDataProviderId = insertDataProvider()

    val eventGroup =
      EventGroup.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.externalMeasurementConsumerId = FIXED_GENERATED_EXTERNAL_ID
          it.providedEventGroupId = PROVIDED_EVENT_GROUP_ID
        }
        .build()

    val exception =
      assertFailsWith<StatusRuntimeException> { eventGroupsService.createEventGroup(eventGroup) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("INVALID_ARGUMENT: MeasurementConsumer not found")
  }

  @Test
  fun `createEventGroup succeeds`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()

    val externalDataProviderId = insertDataProvider()

    val eventGroup =
      EventGroup.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.providedEventGroupId = PROVIDED_EVENT_GROUP_ID
        }
        .build()

    val createdEventGroup = eventGroupsService.createEventGroup(eventGroup)

    // An InternalId for EventGroup is generated.
    copyIdGenerator.generateInternalId()
    // An External for EventGroup is generated.
    val externalEventGroupId = copyIdGenerator.generateExternalId()

    assertThat(createdEventGroup)
      .isEqualTo(
        eventGroup.toBuilder().also { it.externalEventGroupId = externalEventGroupId.value }.build()
      )
  }

  @Test
  fun `getEventGroup succeeds`() = runBlocking {
    val externalMeasurementConsumerId = insertMeasurementConsumer()

    val externalDataProviderId = insertDataProvider()

    val eventGroup =
      EventGroup.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.providedEventGroupId = PROVIDED_EVENT_GROUP_ID
        }
        .build()

    val createdEventGroup = eventGroupsService.createEventGroup(eventGroup)
    // An InternalId for EventGroup is generated.
    copyIdGenerator.generateInternalId()
    // An External for EventGroup is generated.
    val externalEventGroupId = copyIdGenerator.generateExternalId()

    val eventGroupRead =
      eventGroupsService.getEventGroup(
        GetEventGroupRequest.newBuilder()
          .also {
            it.externalDataProviderId = externalDataProviderId
            it.externalEventGroupId = externalEventGroupId.value
          }
          .build()
      )

    assertThat(eventGroupRead).isEqualTo(createdEventGroup)
  }
}

data class EventGroupAndHelperServices<T : EventGroupsCoroutineImplBase>(
  val eventGroupsService: T,
  val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  val dataProvidersService: DataProvidersCoroutineImplBase
)
