/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KmsClient
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.VirtualPersonActivity

@RunWith(JUnit4::class)
class VidLabelerTest {

  private val mockKmsClient: KmsClient = mock()
  private val mockImpressionConverter: ImpressionConverter = mock()

  private fun makeSpec(name: String, blobUri: String): ModelLineSpec =
    ModelLineSpec(
      modelLine = name,
      modelBlobUri = blobUri,
      modelStorageConfig = StorageConfig(),
      activeWindow = ActiveWindow(startMicros = 0L, endMicros = Long.MAX_VALUE),
      config = VidLabelerParams.ModelLineConfig.getDefaultInstance(),
    )

  @Test
  fun `label with multiple model lines uses all of them`() = runBlocking {
    val loadedBlobUris = mutableListOf<String>()
    val vidModelLoader = VidModelLoader { _, modelBlobUri ->
      loadedBlobUris.add(modelBlobUri)
      StubVidAssigner()
    }

    val rawImpressionSource: RawImpressionSource = mock()
    whenever(rawImpressionSource.streamBlobs(any())).then {}

    val labeler =
      VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs =
          listOf(makeSpec("ml1", "gs://models/ml1.bin"), makeSpec("ml2", "gs://models/ml2.bin")),
        overrideModelLines = emptyList(),
        vidModelLoader = vidModelLoader,
        impressionConverter = mockImpressionConverter,
        encryptKmsClient = mockKmsClient,
        encryptKekUri = "fake-kek-uri",
        outputStorageParams = VidLabelerParams.StorageParams.getDefaultInstance(),
        storageConfig = StorageConfig(),
        dataProvider = "dataProviders/edp-1",
      )

    labeler.label()

    assertThat(loadedBlobUris)
      .containsExactly("gs://models/ml1.bin", "gs://models/ml2.bin")
      .inOrder()
  }

  @Test
  fun `label with override model lines uses only those`() = runBlocking {
    val loadedBlobUris = mutableListOf<String>()
    val vidModelLoader = VidModelLoader { _, modelBlobUri ->
      loadedBlobUris.add(modelBlobUri)
      StubVidAssigner()
    }

    val rawImpressionSource: RawImpressionSource = mock()
    whenever(rawImpressionSource.streamBlobs(any())).then {}

    val labeler =
      VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs =
          listOf(
            makeSpec("ml1", "gs://models/ml1.bin"),
            makeSpec("ml2", "gs://models/ml2.bin"),
            makeSpec("ml3", "gs://models/ml3.bin"),
          ),
        overrideModelLines = listOf("ml1", "ml3"),
        vidModelLoader = vidModelLoader,
        impressionConverter = mockImpressionConverter,
        encryptKmsClient = mockKmsClient,
        encryptKekUri = "fake-kek-uri",
        outputStorageParams = VidLabelerParams.StorageParams.getDefaultInstance(),
        storageConfig = StorageConfig(),
        dataProvider = "dataProviders/edp-1",
      )

    labeler.label()

    assertThat(loadedBlobUris)
      .containsExactly("gs://models/ml1.bin", "gs://models/ml3.bin")
      .inOrder()
  }

  @Test
  fun `label with override referencing absent model line throws`() = runBlocking {
    val vidModelLoader = VidModelLoader { _, _ -> StubVidAssigner() }

    val rawImpressionSource: RawImpressionSource = mock()

    val labeler =
      VidLabeler(
        rawImpressionSource = rawImpressionSource,
        modelLineSpecs = listOf(makeSpec("ml1", "gs://models/ml1.bin")),
        overrideModelLines = listOf("nonexistent"),
        vidModelLoader = vidModelLoader,
        impressionConverter = mockImpressionConverter,
        encryptKmsClient = mockKmsClient,
        encryptKekUri = "fake-kek-uri",
        outputStorageParams = VidLabelerParams.StorageParams.getDefaultInstance(),
        storageConfig = StorageConfig(),
        dataProvider = "dataProviders/edp-1",
      )

    val exception = assertFailsWith<IllegalArgumentException> { labeler.label() }
    assertThat(exception).hasMessageThat().contains("nonexistent")
    assertThat(exception).hasMessageThat().contains("not found in model line specs")
  }

  /**
   * [VidAssigner] that returns a fixed VID, for tests that only care about model line resolution.
   */
  private class StubVidAssigner : VidAssigner {
    override fun assign(input: LabelerInput): LabelerOutput =
      LabelerOutput.newBuilder()
        .addPeople(VirtualPersonActivity.newBuilder().setVirtualPersonId(1L).build())
        .build()
  }
}
