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
import java.util.concurrent.atomic.AtomicReference
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
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toOpenEndInstantRange
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.UndigestedEvent
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.ageBucket
import org.wfanet.measurement.edpaggregator.v1alpha.ageRange
import org.wfanet.measurement.edpaggregator.v1alpha.bucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.enumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.labelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.scalarColumn
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
import org.wfanet.measurement.storage.parquetValue
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.labelerOutput
import org.wfanet.virtualpeople.common.virtualPersonActivity

/**
 * Asserts that a report attributes an impression to the demographics of the VID the model assigned,
 * resolved through the model line's `PopulationSpec`, rather than to the demographics the
 * `DataProvider` declared on the raw row.
 *
 * The contract spans two stages that are exercised together only here: the VID labeling write path
 * ([ParquetImpressionConverter] + [PlainVidLabelingSink]) and the `ResultsFulfiller` read path
 * ([StorageEventReader] + [EventProcessingOrchestrator]). Both run for real; only the VID assigner
 * is a stand-in, because the contract concerns what the pipeline does with an assigned VID, not how
 * the model chooses it.
 *
 * The fixture is only meaningful when the assigned VID falls in a **different** subpopulation than
 * the raw row declares. Under an identity correction both readings coincide and the assertion holds
 * whether or not the population attributes are written at all.
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
  fun `report attributes the impression to the assigned VID's demographics`() =
    runBlocking<Unit> {
      val assigner = RecordingVidAssigner(FEMALE_VID)

      val blobDetails = labelOneImpression(assigner)

      // The declared demographics reach the correction model, which is the only place they belong.
      val declared = assigner.lastInput().profileInfo.proprietaryIdSpace1UserInfo.demo.demoBucket
      assertThat(declared.gender).isEqualTo(Gender.GENDER_MALE)
      assertThat(declared.age.minAge).isEqualTo(18)
      assertThat(declared.age.maxAge).isEqualTo(34)

      val results =
        run(
          eventSourceFor(blobDetails),
          listOf(requisitionFilteredOn(FEMALE_35_TO_54_FILTER), requisitionFilteredOn(MALE_FILTER)),
        )

      // Attributed to the assigned VID's subpopulation, on both attributes jointly.
      assertThat(results.getValue(nameFor(FEMALE_35_TO_54_FILTER)).getByteArray().sum())
        .isEqualTo(1)
      // Not to the declared demographics.
      assertThat(results.getValue(nameFor(MALE_FILTER)).getByteArray().sum()).isEqualTo(0)
    }

  @Test
  fun `an identity correction leaves the reported demographics unchanged`() =
    runBlocking<Unit> {
      // Pins the other direction, so the assertion above cannot hold for a reason unrelated to the
      // assigned VID -- every impression being forced into one bucket, say.
      val blobDetails = labelOneImpression(RecordingVidAssigner(MALE_VID))

      val results =
        run(eventSourceFor(blobDetails), listOf(requisitionFilteredOn(MALE_18_TO_34_FILTER)))

      assertThat(results.getValue(nameFor(MALE_18_TO_34_FILTER)).getByteArray().sum()).isEqualTo(1)
    }

  /**
   * Runs one raw row declaring MALE / 18-34 through the real converter and sink, returning the
   * `BlobDetails` sidecar the sink wrote.
   */
  private suspend fun labelOneImpression(assigner: VidAssigner): BlobDetails {
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
              assigner = assigner,
              config = MODEL_LINE_CONFIG,
              rankIndex = null,
            )
          ),
        impressionConverter = ParquetImpressionConverter(TestEvent.getDescriptor(), writer),
        fileMetadata = RawImpressionFileMetadata(eventDate = EVENT_DATE),
        encryptKmsClient = kmsClient,
        encryptKekUri = kekUri,
        outputStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = "test-project"
            impressionsBlobPrefix = "file:///labeled"
          },
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
    sink.processBatch(
      listOf(
        UndigestedEvent(
          row =
            mapOf(
              EVENT_ID_COLUMN to parquetValue { stringValue = "event-1" },
              EVENT_TIME_COLUMN to parquetValue { int64Value = EVENT_TIME_MICROS },
              PERSON_ID_COLUMN to parquetValue { stringValue = "person-1" },
              GENDER_COLUMN to parquetValue { stringValue = DECLARED_GENDER },
              AGE_GROUP_COLUMN to parquetValue { stringValue = DECLARED_AGE_GROUP },
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

  /** Reads the sink's encrypted output back through the fulfiller's own reader. */
  private fun eventSourceFor(blobDetails: BlobDetails): SingleGroupEventSource =
    SingleGroupEventSource(
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = tempFolder.root),
        descriptor = TestEvent.getDescriptor(),
      ),
      EVENT_GROUP,
    )

  private suspend fun run(eventSource: SingleGroupEventSource, requisitions: List<Requisition>) =
    orchestrator.run(
      eventSource = eventSource,
      vidIndexMap = InMemoryVidIndexMap.build(POPULATION_SPEC),
      populationSpec = POPULATION_SPEC,
      requisitions = requisitions,
      eventGroupSelector =
        FilterSpecIndex.Companion.EventGroupSelector.ByEventGroupReferenceIds(
          mapOf(EVENT_GROUP to EVENT_GROUP)
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
                      startTime = COLLECTION_INTERVAL.start.toProtoTime()
                      endTime = COLLECTION_INTERVAL.endExclusive.toProtoTime()
                    }
                  this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                }
            }
        }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
    }
    return requisition {
      name = nameFor(filter)
      measurement = "measurements/test-measurement"
      state = Requisition.State.UNFULFILLED
      measurementSpec = signedMessage { message = MEASUREMENT_SPEC.pack() }
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
      encryptedRequisitionSpec =
        encryptRequisitionSpec(signRequisitionSpec(spec, MC_SIGNING_KEY), DATA_PROVIDER_PUBLIC_KEY)
    }
  }

  /**
   * Returns [vid] for every input and records the last [LabelerInput] it saw, so a test can assert
   * the declared demographics actually reached the model.
   */
  private class RecordingVidAssigner(private val vid: Long) : VidAssigner {
    private val last = AtomicReference<LabelerInput>()

    fun lastInput(): LabelerInput = checkNotNull(last.get()) { "assigner was never called" }

    override fun assign(input: LabelerInput): LabelerOutput {
      last.set(input)
      return labelerOutput { people += virtualPersonActivity { virtualPersonId = vid } }
    }
  }

  /** Wraps a [StorageEventReader] as an [EventSource] tagged with this test's event group. */
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
    private const val GENDER_COLUMN = "person_gender"
    private const val AGE_GROUP_COLUMN = "person_age_group"

    /** What the raw row claims about this person. The model disagrees. */
    private const val DECLARED_GENDER = "MALE"
    private const val DECLARED_AGE_GROUP = "YEARS_18_TO_34"

    private val EVENT_DATE: LocalDate = LocalDate.of(2026, 6, 30)
    private val EVENT_TIME_MICROS: Long =
      EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000L
    private val COLLECTION_INTERVAL = (EVENT_DATE..EVENT_DATE).toOpenEndInstantRange()

    /** A VID in the MALE 18-34 subpopulation -- the one the raw row declares. */
    private const val MALE_VID = 42L

    /** A VID in the FEMALE 35-54 subpopulation -- a different one. */
    private const val FEMALE_VID = 150L

    private val MALE_FILTER = "person.gender == ${Person.Gender.MALE_VALUE}"
    private val MALE_18_TO_34_FILTER =
      "person.gender == ${Person.Gender.MALE_VALUE} && " +
        "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    private val FEMALE_35_TO_54_FILTER =
      "person.gender == ${Person.Gender.FEMALE_VALUE} && " +
        "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"

    private fun nameFor(filter: String): String = "requisitions/${filter.hashCode().toUInt()}"

    /**
     * Two subpopulations with disjoint VID ranges and different demographics, so an assigned VID
     * identifies exactly one of them. `Person` carries three population attributes and
     * `PopulationSpecValidator` requires all of them on every subpopulation.
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
     * Carries the declared demographics to the correction model via `labeler_input_field_mapping`
     * and nowhere else. There is deliberately no `event_template_field_mapping` onto `person.*`:
     * population attributes are sourced from [POPULATION_SPEC], and `EventMessageMapper` rejects a
     * mapping that targets one.
     */
    private val MODEL_LINE_CONFIG: VidLabelerParams.ModelLineConfig =
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping += labelerInputFieldMapping {
          fieldPath = "event_id.id"
          scalar = scalarColumn { column = EVENT_ID_COLUMN }
        }
        labelerInputFieldMapping += labelerInputFieldMapping {
          fieldPath = "timestamp_usec"
          scalar = scalarColumn { column = EVENT_TIME_COLUMN }
        }
        labelerInputFieldMapping += labelerInputFieldMapping {
          fieldPath = "profile_info.proprietary_id_space_1_user_info.user_id"
          scalar = scalarColumn { column = PERSON_ID_COLUMN }
        }
        labelerInputFieldMapping += labelerInputFieldMapping {
          fieldPath = "profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.gender"
          enumLookup = enumLookup {
            column = GENDER_COLUMN
            lookupTable["MALE"] = "GENDER_MALE"
            lookupTable["FEMALE"] = "GENDER_FEMALE"
          }
        }
        labelerInputFieldMapping += labelerInputFieldMapping {
          fieldPath = "profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.age"
          ageRange = ageRange {
            bucketLookup = bucketLookup {
              column = AGE_GROUP_COLUMN
              bucketTable["YEARS_18_TO_34"] = ageBucket {
                minAge = 18
                maxAge = 34
              }
              bucketTable["YEARS_35_TO_54"] = ageBucket {
                minAge = 35
                maxAge = 54
              }
            }
          }
        }
        optionalEntityKeyFieldMapping["person"] = PERSON_ID_COLUMN
      }

    private val MEASUREMENT_SPEC = measurementSpec {
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
      loadSigningKey(
        SECRET_FILES_PATH.resolve("${MEASUREMENT_CONSUMER_ID}_cs_cert.der").toFile(),
        SECRET_FILES_PATH.resolve("${MEASUREMENT_CONSUMER_ID}_cs_private.der").toFile(),
      )
  }
}
