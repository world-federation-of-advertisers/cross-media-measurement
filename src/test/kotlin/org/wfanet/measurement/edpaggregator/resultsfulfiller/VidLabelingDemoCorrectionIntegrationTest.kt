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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Message
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneOffset
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.UndigestedEvent
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.BaseVidLabelingSink
import org.wfanet.measurement.edpaggregator.vidlabeler.ModelLineContext
import org.wfanet.measurement.edpaggregator.vidlabeler.ParquetImpressionConverter
import org.wfanet.measurement.edpaggregator.vidlabeler.PlainVidLabelingSink
import org.wfanet.measurement.edpaggregator.vidlabeler.PopulationAttributeWriter
import org.wfanet.measurement.edpaggregator.vidlabeler.VidAssigner
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerMetrics
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.labelerOutput
import org.wfanet.virtualpeople.common.virtualPersonActivity

/**
 * Chains the two halves of the pipeline that no other test connects: the **real** VID labeling
 * write path and the **real** `ResultsFulfiller` event-processing path.
 *
 * The specific defect this guards against (see issue #4384) is the labeled output carrying the
 * demographics the `DataProvider` uploaded instead of the ones the model assigned. Every existing
 * test misses it:
 * * the labeler's own tests ([org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelingSinkTest]
 *   and friends) stop at the labeled blob and never involve `ResultsFulfiller`;
 * * [EventProcessingIntegrationTest] hand-builds its `LabeledImpression`s, setting `vid` and
 *   `event` consistently by hand, so its fixtures cannot express the disagreement at all;
 * * the in-process EDPA harness seeds already-labeled impressions generated *from* the same
 *   `PopulationSpec` the fulfiller reads, so uploaded and assigned demographics coincide by
 *   construction.
 *
 * The load-bearing part of this fixture is therefore that the assigned VID falls in a **different**
 * subpopulation than the row declared. With an identity correction the test passes whether or not
 * the production code is correct, which is exactly how the bug shipped.
 */
@RunWith(JUnit4::class)
class VidLabelingDemoCorrectionIntegrationTest {
  @get:Rule val tempFolder = TemporaryFolder()

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "/vid-labeling-key"
  private lateinit var kmsClient: KmsClient
  private lateinit var orchestrator: EventProcessingOrchestrator<Message>

  @Before
  fun initOrchestrator() {
    kmsClient = TestEncryptedStorage.buildFakeKmsClient(kekUri, keyTemplate = "AES128_GCM")
    orchestrator = EventProcessingOrchestrator(PRIVATE_ENCRYPTION_KEY)
  }

  @Test
  fun `report counts the impression under the demo the model assigned, not the one uploaded`() =
    runBlocking<Unit> {
      // The raw row declares MALE 18-34; the model assigns a VID from the FEMALE 35-54 pool.
      val blobDetails = labelOneImpression(assignedVid = FEMALE_VID)

      val eventSource = eventSourceFor(blobDetails)
      val femaleRequisition = requisitionFilteredOn("person.gender == ${FEMALE_VALUE}")
      val maleRequisition = requisitionFilteredOn("person.gender == ${MALE_VALUE}")

      val results = run(eventSource, listOf(femaleRequisition, maleRequisition))

      // The impression is reported under the model's demo...
      assertThat(results.getValue(femaleRequisition.name).getByteArray().sum()).isEqualTo(1)
      // ...and NOT under the demographics the DataProvider uploaded. Before the fix this was
      // reversed: the labeled event carried the raw row's MALE and the female slice was empty.
      assertThat(results.getValue(maleRequisition.name).getByteArray().sum()).isEqualTo(0)
    }

  @Test
  fun `an uncorrected impression is reported under its own demo`() =
    runBlocking<Unit> {
      // Control for the assertion above: with an identity correction -- the model assigning a VID
      // from the same subpopulation the row declared -- the reported demo is unchanged. Without
      // this
      // case the first test alone could pass for the wrong reason, e.g. if every impression were
      // being forced into the FEMALE bucket regardless of the VID.
      val blobDetails = labelOneImpression(assignedVid = MALE_VID)

      val eventSource = eventSourceFor(blobDetails)
      val maleRequisition = requisitionFilteredOn("person.gender == ${MALE_VALUE}")

      val results = run(eventSource, listOf(maleRequisition))

      assertThat(results.getValue(maleRequisition.name).getByteArray().sum()).isEqualTo(1)
    }

  /**
   * Runs one raw row through the real [ParquetImpressionConverter] and [PlainVidLabelingSink],
   * returning the `BlobDetails` sidecar the sink wrote.
   *
   * [assignedVid] is what the (fake) model returns. A fake [VidAssigner] is enough because the
   * defect is not in the model: it is in what the pipeline does with the model's VID. Using a
   * compiled VirtualPeople model here would only add a correction matrix that this test would then
   * have to reverse-engineer to know the expected answer.
   */
  private suspend fun labelOneImpression(assignedVid: Long): BlobDetails {
    val writer = PopulationAttributeWriter(TestEvent.getDescriptor(), POPULATION_SPEC)
    val sink =
      PlainVidLabelingSink(
        inputBlobUri = "file:///raw/impressions-1.parquet",
        modelLineContexts =
          listOf(
            ModelLineContext(
              modelLine = MODEL_LINE,
              activeWindow =
                ActiveWindow(startMicros = EVENT_TIME_MICROS, endMicros = EVENT_TIME_MICROS + 1),
              assigner = FixedVidAssigner(assignedVid),
              config = MODEL_LINE_CONFIG,
              rankIndex = null,
            )
          ),
        impressionConverter = ParquetImpressionConverter(TestEvent.getDescriptor(), writer),
        fileMetadata = RawImpressionFileMetadata(eventDate = EVENT_DATE),
        encryptKmsClient = kmsClient,
        encryptKekUri = kekUri,
        outputStorageParams =
          VidLabelerParams.StorageParams.newBuilder()
            .setGcsProjectId("test-project")
            .setImpressionsBlobPrefix("file:///labeled")
            .build(),
        storageConfig = StorageConfig(rootDirectory = tempFolder.root),
        dataProvider = DATA_PROVIDER,
        metrics =
          VidLabelerMetrics(
            SdkMeterProvider.builder()
              .registerMetricReader(InMemoryMetricReader.create())
              .build()
              .get("test")
          ),
        encryptionKeySemaphore = Semaphore(BaseVidLabelingSink.DEFAULT_ENCRYPTION_KEY_PARALLELISM),
      )

    tempFolder.root.resolve("labeled").mkdirs()
    // The declared demographics ride in on the raw row. They reach the correction model through
    // labeler_input_field_mapping; they can never reach the output event, because
    // EventMessageMapper rejects an event_template_field_mapping onto a population attribute.
    sink.processBatch(
      listOf(
        UndigestedEvent(
          row =
            mapOf(
              EVENT_ID_COLUMN to ParquetValue.newBuilder().setStringValue("event-1").build(),
              EVENT_TIME_COLUMN to
                ParquetValue.newBuilder().setInt64Value(EVENT_TIME_MICROS).build(),
              PERSON_ID_COLUMN to ParquetValue.newBuilder().setStringValue("person-1").build(),
            )
        )
      )
    )
    sink.commit()
    sink.close()

    val metadataFile =
      tempFolder.root.walkTopDown().single { it.isFile && it.name.endsWith(".metadata.binpb") }
    return BlobDetails.parseFrom(metadataFile.readBytes())
  }

  /** Reads the sink's real (encrypted) output back through the fulfiller's own reader. */
  private fun eventSourceFor(blobDetails: BlobDetails): SingleGroupEventSource {
    val reader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = tempFolder.root),
        descriptor = TestEvent.getDescriptor(),
      )
    return SingleGroupEventSource(reader, EVENT_GROUP)
  }

  private suspend fun run(eventSource: SingleGroupEventSource, requisitions: List<Requisition>) =
    orchestrator.run(
      eventSource = eventSource,
      vidIndexMap = InMemoryVidIndexMap.build(POPULATION_SPEC),
      populationSpec = POPULATION_SPEC,
      requisitions = requisitions,
      eventGroupSelector =
        FilterSpecIndex.Companion.EventGroupSelector.ByEventGroupReferenceIds(
          requisitions.associate { EVENT_GROUP to EVENT_GROUP }
        ),
      config =
        PipelineConfiguration(
          batchSize = 10,
          channelCapacity = 100,
          threadPoolSize = 2,
          workers = 2,
          readConcurrency = 2,
        ),
      eventDescriptor = TestEvent.getDescriptor(),
    )

  private fun requisitionFilteredOn(filter: String): Requisition {
    val measurementSpec = measurementSpec {
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1E-12
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1E-12
          }
          maximumFrequency = 10
        }
    }
    val timeRange = OpenEndTimeRange.fromClosedDateRange(EVENT_DATE..EVENT_DATE)
    return requisition {
      name = "requisitions/${filter.hashCode().toUInt()}"
      measurement = "measurements/test-measurement"
      state = Requisition.State.UNFULFILLED
      this.measurementSpec = signedMessage { message = measurementSpec.pack() }
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                noiseMechanisms += ProtocolConfig.NoiseMechanism.NONE
                deterministicCountDistinct =
                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                deterministicDistribution =
                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
              }
          }
      }
      val spec = requisitionSpec {
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = EVENT_GROUP
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval =
                      com.google.type.interval {
                        startTime = timeRange.start.toProtoTime()
                        endTime = timeRange.endExclusive.toProtoTime()
                      }
                    this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                  }
              }
          }
        measurementPublicKey = MC_PUBLIC_KEY.pack()
        nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
      }
      encryptedRequisitionSpec =
        encryptRequisitionSpec(signRequisitionSpec(spec, MC_SIGNING_KEY), DATA_PROVIDER_PUBLIC_KEY)
    }
  }

  /**
   * Wraps the fulfiller's own [StorageEventReader] as an [EventSource], tagging every batch with
   * this test's event group. Mirrors the equivalent helper in [EventProcessingIntegrationTest].
   */
  private class SingleGroupEventSource(
    private val reader: EventReader<Message>,
    private val eventGroupReferenceId: String,
  ) : EventSource<Message> {
    override fun generateEventBatches(): Flow<EventBatch<Message>> = flow {
      reader.readEvents().collect { events ->
        if (events.isNotEmpty()) {
          val times = events.map { it.timestamp }
          emit(
            EventBatch(
              events,
              times.min(),
              times.max(),
              eventGroupIdentifier = EventGroupIdentifier.ByReferenceId(eventGroupReferenceId),
            )
          )
        }
      }
    }
  }

  /** Returns [vid] for every input, standing in for the model's demographic correction. */
  private class FixedVidAssigner(private val vid: Long) : VidAssigner {
    override fun assign(input: LabelerInput): LabelerOutput = labelerOutput {
      people += virtualPersonActivity { virtualPersonId = vid }
    }
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private const val DATA_PROVIDER = "dataProviders/edp-1"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val EVENT_GROUP = "event-group-1"
    private const val EVENT_ID_COLUMN = "event_id"
    private const val EVENT_TIME_COLUMN = "event_time_micros"
    private const val PERSON_ID_COLUMN = "person_id"

    private val EVENT_DATE: LocalDate = LocalDate.of(2026, 6, 30)
    private val EVENT_TIME_MICROS: Long =
      EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000L

    private val MALE_VALUE = Person.Gender.MALE_VALUE
    private val FEMALE_VALUE = Person.Gender.FEMALE_VALUE

    /** A VID inside the MALE 18-34 subpopulation. */
    private const val MALE_VID = 42L
    /** A VID inside the FEMALE 35-54 subpopulation -- a different demo than the row declares. */
    private const val FEMALE_VID = 150L

    /**
     * Two subpopulations with disjoint VID ranges and *different* demographics, so an assigned VID
     * unambiguously identifies one of them. `Person` carries three population attributes, all of
     * which must be set or `PopulationSpecValidator` rejects the spec.
     */
    private val POPULATION_SPEC: PopulationSpec = populationSpec {
      subpopulations += subPopulation(Person.Gender.MALE, Person.AgeGroup.YEARS_18_TO_34, 1L, 100L)
      subpopulations +=
        subPopulation(Person.Gender.FEMALE, Person.AgeGroup.YEARS_35_TO_54, 101L, 200L)
    }

    private fun subPopulation(
      gender: Person.Gender,
      ageGroup: Person.AgeGroup,
      startVid: Long,
      endVid: Long,
    ): PopulationSpec.SubPopulation =
      PopulationSpecKt.subPopulation {
        attributes +=
          ProtoAny.pack(
            person {
              this.gender = gender
              this.ageGroup = ageGroup
              socialGradeGroup = Person.SocialGradeGroup.A_B_C1
            }
          )
        vidRanges +=
          PopulationSpecKt.vidRange {
            this.startVid = startVid
            endVidInclusive = endVid
          }
      }

    /**
     * Note what is absent: no `event_template_field_mapping` onto `person.*`. Population attributes
     * come from [POPULATION_SPEC] via `PopulationAttributeWriter`; the declared demographics reach
     * the model through `labeler_input_field_mapping` instead.
     */
    private val MODEL_LINE_CONFIG: VidLabelerParams.ModelLineConfig =
      VidLabelerParams.ModelLineConfig.newBuilder()
        .addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn(EVENT_ID_COLUMN))
            .build()
        )
        .addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("timestamp_usec")
            .setScalar(ScalarColumn.newBuilder().setColumn(EVENT_TIME_COLUMN))
            .build()
        )
        .addLabelerInputFieldMapping(
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("profile_info.proprietary_id_space_1_user_info.user_id")
            .setScalar(ScalarColumn.newBuilder().setColumn(PERSON_ID_COLUMN))
            .build()
        )
        .putOptionalEntityKeyFieldMapping("person", PERSON_ID_COLUMN)
        .build()

    private val SECRET_FILES_PATH =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val EDP_DISPLAY_NAME = "edp1"
    private const val MEASUREMENT_CONSUMER_ID = "mc"

    private val PRIVATE_ENCRYPTION_KEY =
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_SIGNING_KEY: SigningKeyHandle =
      org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve("${MEASUREMENT_CONSUMER_ID}_cs_cert.der").toFile(),
        SECRET_FILES_PATH.resolve("${MEASUREMENT_CONSUMER_ID}_cs_private.der").toFile(),
      )
  }
}
