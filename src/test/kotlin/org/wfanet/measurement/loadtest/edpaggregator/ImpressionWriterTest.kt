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

package org.wfanet.measurement.loadtest.edpaggregator

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.time.LocalDate
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class ImpressionWriterTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `verifies that data is written correctly by ds`() {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsClient = run {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      client
    }
    tempFolder.root.resolve("some-impression-bucket").mkdirs()
    tempFolder.root.resolve("some-metadata-bucket").mkdirs()
    val impressionWriter =
      ImpressionsWriter(
        "some-event-group-path",
        kekUri,
        kmsClient,
        "some-impression-bucket",
        "some-metadata-bucket",
        tempFolder.root,
        "file:///",
      )
    val events: Flow<DateShardedLabeledImpression> =
      flowOf(
        DateShardedLabeledImpression(LocalDate.parse("2020-01-01"), flowOf(labeledImpression {})),
        DateShardedLabeledImpression(LocalDate.parse("2020-01-02"), flowOf(labeledImpression {})),
      )
    runBlocking { impressionWriter.writeLabeledImpressionData(events) }
    val client = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      listOf("2020-01-01", "2020-01-02").forEach {
        assertNotNull(client.getBlob("some-metadata-bucket/ds/$it/some-event-group-path/metadata"))
        assertNotNull(
          client.getBlob("some-impression-bucket/ds/$it/some-event-group-path/impressions")
        )
      }
    }
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
