package org.wfanet.panelmatch.client.exchangetasks.emr

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.*
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails

const val TEST_APPLICATION_ID = "test-application-id"
const val TEST_EXCHANGE_ATTEMPT = "recurringExchanges/test-recurring-exchange/exchanges/" +
  "test-exchange/steps/test-step/attempts/test-step-attempt"

@RunWith(JUnit4::class)
class EmrExchangeTaskServiceTest {

  @Mock
  private lateinit var mockStorageClient1: StorageClient

  @Mock
  private lateinit var emrServerlessClientService1: EmrServerlessClientService

  @Mock
  private lateinit var mockStorageClient2: StorageClient

  @Mock
  private lateinit var emrServerlessClientService2: EmrServerlessClientService

  private lateinit var emrExchangeTaskService: EmrExchangeTaskService

  @Before
  fun setUp() {
    MockitoAnnotations.openMocks(this).close()

    runBlocking {
      whenever(mockStorageClient1.getBlob(any())).thenReturn(null)
      whenever(mockStorageClient1.writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8()))).thenReturn(null)
      whenever(emrServerlessClientService1.createApplication(any())).thenReturn(TEST_APPLICATION_ID)

      emrExchangeTaskService = EmrExchangeTaskService(
        exchangeTaskAppIdPath = "",
        exchangeWorkflowPrefix = "",
        storageClient = mockStorageClient1,
        storageType = StorageDetails.PlatformCase.AWS,
        emrServerlessClientService = emrServerlessClientService1
      )
    }
  }

  @Test
  fun testUseExistingEmrAppIdFromBlob() {
    runBlocking {
      val applicationIdBlob: StorageClient.Blob = object: StorageClient.Blob {
        override val size: Long
          get() = TEST_APPLICATION_ID.toByteStringUtf8().size().toLong()
        override val storageClient: StorageClient
          get() = mockStorageClient2

        override suspend fun delete() {}

        override fun read(): Flow<ByteString> {
          return flow {
            TEST_APPLICATION_ID.toByteStringUtf8()
          }
        }
      }

      whenever(mockStorageClient2.getBlob(any())).thenReturn(applicationIdBlob)

      EmrExchangeTaskService(
        exchangeTaskAppIdPath = "",
        exchangeWorkflowPrefix = "",
        storageClient = mockStorageClient2,
        storageType = StorageDetails.PlatformCase.AWS,
        emrServerlessClientService = emrServerlessClientService2
      )

      verify(mockStorageClient2, times(1)).getBlob(any())
      verify(mockStorageClient2, times(0)).writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8()))
      verify(emrServerlessClientService2, times(0)).createApplication(any())
    }
  }

  @Test
  fun testRunPanelExchangeStepOnEmrAppFor() {
    runBlocking {
      whenever(emrServerlessClientService1.startOrStopApplication(any(), any())).thenReturn(true)
      whenever(emrServerlessClientService1.startAndWaitJobRunCompletion(any(), any())).thenReturn(true)

      emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
        "",
        0,
        CanonicalExchangeStepAttemptKey.fromName(TEST_EXCHANGE_ATTEMPT)!!,
        LocalDate.now()
      )

      verify(mockStorageClient1, times(1)).getBlob(any())
      verify(emrServerlessClientService1, times(1)).createApplication(any())
      verify(mockStorageClient1, times(1)).writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8()))
      verify(emrServerlessClientService1, times(2)).startOrStopApplication(any(), any())
      verify(emrServerlessClientService1, times(1)).startAndWaitJobRunCompletion(any(), any())
    }
  }

  @Test(expected = RuntimeException::class)
  fun testRunPanelExchangeStepOnEmrAppThrowsExceptionWhenStartApplicationFails() {
    runBlocking {
      whenever(emrServerlessClientService1.startOrStopApplication(any(), eq(true))).thenReturn(false)

      emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
        "",
        0,
        CanonicalExchangeStepAttemptKey.fromName(TEST_EXCHANGE_ATTEMPT)!!,
        LocalDate.now()
      )
    }
  }

  @Test(expected = RuntimeException::class)
  fun testRunPanelExchangeStepOnEmrAppThrowsExceptionWhenRunApplicationFails() {
    runBlocking {
      whenever(emrServerlessClientService1.startOrStopApplication(any(), any())).thenReturn(true)
      whenever(emrServerlessClientService1.startAndWaitJobRunCompletion(any(), any())).thenReturn(false)


      emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
        "",
        0,
        CanonicalExchangeStepAttemptKey.fromName(TEST_EXCHANGE_ATTEMPT)!!,
        LocalDate.now()
      )
    }
  }

  @Test(expected = RuntimeException::class)
  fun testRunPanelExchangeStepOnEmrAppThrowsExceptionWhenStopApplicationFails() {
    runBlocking {
      whenever(emrServerlessClientService1.startOrStopApplication(any(), eq(true))).thenReturn(true)
      whenever(emrServerlessClientService1.startAndWaitJobRunCompletion(any(), any())).thenReturn(true)
      whenever(emrServerlessClientService1.startOrStopApplication(any(), eq(false))).thenReturn(false)


      emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
        "",
        0,
        CanonicalExchangeStepAttemptKey.fromName(TEST_EXCHANGE_ATTEMPT)!!,
        LocalDate.now()
      )
    }
  }
}
