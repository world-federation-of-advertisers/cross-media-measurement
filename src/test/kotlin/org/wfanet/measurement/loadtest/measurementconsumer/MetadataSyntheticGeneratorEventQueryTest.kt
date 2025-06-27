// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import org.jetbrains.annotations.Blocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.loadtest.dataprovider.EventQuery

@RunWith(JUnit4::class)
class MetadataSyntheticGeneratorEventQueryTest {

  @Test
  fun `getLabeledEvents returns events distributed across date range`() {
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val metadataSyntheticGeneratorEventQuery =
      MetadataSyntheticGeneratorEventQuery(syntheticPopulationSpec, MC_ENCRYPTION_PRIVATE_KEY)
    val dateRange = syntheticEventGroupSpec.getDateSpecs(0).dateRange
    val startTime =
      LocalDate.of(dateRange.start.year, dateRange.start.month, dateRange.start.day)
        .atStartOfDay(ZONE_ID)
        .toInstant()
    val endTime =
      LocalDate.of(
          dateRange.endExclusive.year,
          dateRange.endExclusive.month,
          dateRange.endExclusive.day - 1,
        )
        .atTime(23, 59, 59)
        .atZone(ZONE_ID)
        .toInstant()
    val eg = eventGroup {
      encryptedMetadata =
        encryptMetadata(
          metadata {
            eventGroupMetadataDescriptor = TestEvent.getDescriptor().name
            metadata = Any.pack(syntheticEventGroupSpec)
          },
          measurementConsumerPublicKey = MC_PUBLIC_KEY,
        )
    }

    val eventGroupSpec =
      EventQuery.EventGroupSpec(
        eg,
        RequisitionSpecKt.EventGroupEntryKt.value {
          collectionInterval = interval {
            this.startTime = startTime.toProtoTime()
            this.endTime = endTime.toProtoTime()
          }
        },
      )
    val events = metadataSyntheticGeneratorEventQuery.getLabeledEvents(eventGroupSpec)
    assertThat(events.toList()).hasSize(8001)
  }

  companion object {
    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!
    private const val REPO_NAME = "wfa_measurement_system"
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(Paths.get(REPO_NAME, "src", "main", "k8s", "testing", "secretfiles"))
      )
    private const val MC_DISPLAY_NAME = "mc"
    private val MC_ENCRYPTION_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")

    @Blocking
    private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
      return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
    }

    val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val ZONE_ID: ZoneId = ZoneOffset.UTC
  }
}
