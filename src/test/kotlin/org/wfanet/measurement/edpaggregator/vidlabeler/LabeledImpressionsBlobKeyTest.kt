// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class LabeledImpressionsBlobKeyTest {
  @Test
  fun `output URIs share a directory when prefix has trailing slash`() {
    val dataUri =
      LabeledImpressionsBlobKeys.forInputUri(
        OUTPUT_PREFIX,
        "gs://raw-bucket/path/input.parquet",
        MODEL_LINE,
        EVENT_DATE,
      )
    val sidecarUri = "$dataUri.metadata.binpb"
    val doneUri =
      LabeledImpressionsBlobKeys.forDoneUri(
        OUTPUT_PREFIX,
        RAW_IMPRESSION_UPLOAD,
        MODEL_LINE,
        EVENT_DATE,
      )
    val expectedDirectory = "gs://bucket/labeled/model-line/ml1/2026-09-10"

    assertThat(dataUri.substringBeforeLast('/')).isEqualTo(expectedDirectory)
    assertThat(sidecarUri.substringBeforeLast('/')).isEqualTo(expectedDirectory)
    assertThat(doneUri.substringBeforeLast('/')).isEqualTo(expectedDirectory)
    assertThat(doneUri).startsWith("$expectedDirectory/done-")
  }

  @Test
  fun `done URI is stable per upload and distinct across same-date uploads`() {
    val mainUpload =
      LabeledImpressionsBlobKeys.forDoneUri(
        OUTPUT_PREFIX,
        RAW_IMPRESSION_UPLOAD,
        MODEL_LINE,
        EVENT_DATE,
      )
    val advertiserUpload =
      LabeledImpressionsBlobKeys.forDoneUri(
        OUTPUT_PREFIX,
        "dataProviders/dp/rawImpressionUploads/advertiser",
        MODEL_LINE,
        EVENT_DATE,
      )

    assertThat(
        LabeledImpressionsBlobKeys.forDoneUri(
          OUTPUT_PREFIX,
          RAW_IMPRESSION_UPLOAD,
          MODEL_LINE,
          EVENT_DATE,
        )
      )
      .isEqualTo(mainUpload)
    assertThat(advertiserUpload).isNotEqualTo(mainUpload)
    assertThat(advertiserUpload.substringBeforeLast('/'))
      .isEqualTo(mainUpload.substringBeforeLast('/'))
  }

  companion object {
    private const val OUTPUT_PREFIX = "gs://bucket/labeled/"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
    private const val RAW_IMPRESSION_UPLOAD = "dataProviders/dp/rawImpressionUploads/main"
    private val EVENT_DATE = LocalDate.of(2026, 9, 10)
  }
}
