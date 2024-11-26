package org.wfanet.measurement.storage.testing

import org.junit.Test
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.securecomputation.DataWatcher
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.GcsSubscribingStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.core.labeler.LabelerInput

class SimpleIntegrationTest {

    @Test
    fun vidLabel() = runBlocking {
        val testPath = "temp/test/path"
        val dataWatcherConfig = DataWatcherConfig.newBuilder().setSourcePathRegex(testPath).build()
        val dataWatcher = DataWatcher(listOf(dataWatcherConfig))

        val inMemoryStorageClient = InMemoryStorageClient()
        val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)

        val testData = "Test Labeler Input Data"
        val labelerInput = LabelerInput.newBuilder().setEventId(testData).build()
        mesosRecordIoStorageClient.writeBlob(testPath, flowOf(ByteString.copyFromUtf8(labelerInput.eventId)))

        // Additional test verifications would be performed here.
    }
}
