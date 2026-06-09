package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message
import java.nio.ByteOrder
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import kotlin.math.abs
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.virtualpeople.common.AgeRange
import org.wfanet.virtualpeople.common.DemoBucket
import org.wfanet.virtualpeople.common.DemoInfo
import org.wfanet.virtualpeople.common.EventId
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.ProfileInfo
import org.wfanet.virtualpeople.common.UserInfo
import org.wfanet.virtualpeople.core.labeler.Labeler

object ReferenceVidDataGeneration {
  private val FINGERPRINT_FUNCTION = Hashing.farmHashFingerprint64()
  private const val SECONDS_PER_DAY = 86400

  /**
   * Generates events by running input IDs through a VID [labeler] model.
   *
   * Each input ID is converted to a string and passed through the labeler with the demographic
   * profile from the spec. The labeler assigns VIDs via hash-based routing and a demographic
   * correction matrix.
   *
   * The output is the same type as [SyntheticDataGeneration.generateEvents], so it feeds directly
   * into [ImpressionsWriter] and the rest of the pipeline.
   *
   * @param labeler the VID labeler built from a CompiledNode model
   * @param messageInstance event message prototype with template fields
   * @param populationSpec population spec for validation — output VIDs must fall within its ranges
   * @param spec the reference VID event group spec defining input IDs and demographics
   * @param zoneId timezone for date shards
   */
  fun <T : Message> generateEvents(
    labeler: Labeler,
    messageInstance: T,
    populationSpec: PopulationSpec,
    spec: ReferenceVidEventGroupSpec,
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Sequence<LabeledEventDateShard<T>> {
    val rawLabeledVids = labelAllInputs(labeler, spec)
    val labeledVids = validateAgainstPopulationSpec(rawLabeledVids, populationSpec)

    val templateFieldsByTypeUrl = buildTemplateFieldsByTypeUrl(messageInstance)
    val subPopulationPrototypes: Map<Int, T> =
      buildSubPopulationPrototypes(messageInstance, populationSpec, templateFieldsByTypeUrl)

    return sequence {
      for (dateSpec in spec.dateSpecsList) {
        val dateProgression = dateSpec.dateRange.toProgression()
        val numDays =
          ChronoUnit.DAYS.between(dateProgression.start, dateProgression.endInclusive) + 1

        for (date in dateProgression) {
          val events: Sequence<LabeledEvent<T>> =
            generateDayEvents(
              labeledVids,
              subPopulationPrototypes,
              spec.nonPopulationFieldValuesMap,
              dateProgression,
              date,
              dateSpec.frequency.toInt(),
              numDays.toInt(),
              zoneId,
            )
          yield(LabeledEventDateShard(date, events))
        }
      }
    }
  }

  /** Result of labeling a single input ID. */
  data class LabeledVidResult(
    val inputId: Long,
    val vid: Long,
    val outputGender: Gender,
    val outputMinAge: Int,
    val outputMaxAge: Int,
    val subPopulationIndex: Int,
  )

  /** Runs all input IDs through the labeler and collects results. */
  fun labelAllInputs(labeler: Labeler, spec: ReferenceVidEventGroupSpec): List<LabeledVidResult> {
    val results = mutableListOf<LabeledVidResult>()

    for (demoDist in spec.demographicDistributionsList) {
      val gender = Gender.forNumber(demoDist.gender) ?: error("Invalid gender: ${demoDist.gender}")

      for (id in demoDist.idRange.start until demoDist.idRange.endExclusive) {
        val input =
          LabelerInput.newBuilder()
            .apply {
              eventId = EventId.newBuilder().setId(id.toString()).build()
              timestampUsec = 0L
              profileInfo =
                ProfileInfo.newBuilder()
                  .apply {
                    proprietaryIdSpace1UserInfo =
                      UserInfo.newBuilder()
                        .apply {
                          userId = id.toString()
                          demo =
                            DemoInfo.newBuilder()
                              .apply {
                                demoBucket =
                                  DemoBucket.newBuilder()
                                    .apply {
                                      this.gender = gender
                                      age =
                                        AgeRange.newBuilder()
                                          .setMinAge(demoDist.minAge)
                                          .setMaxAge(demoDist.maxAge)
                                          .build()
                                    }
                                    .build()
                              }
                              .build()
                        }
                        .build()
                  }
                  .build()
            }
            .build()

        val output = labeler.label(input)
        check(output.peopleCount > 0) { "Labeler returned no people for input ID $id" }
        val person = output.getPeople(0)
        check(person.virtualPersonId > 0) { "Labeler returned VID 0 for input ID $id" }

        results.add(
          LabeledVidResult(
            inputId = id,
            vid = person.virtualPersonId.toLong(),
            outputGender = person.label.demo.gender,
            outputMinAge = person.label.demo.age.minAge,
            outputMaxAge = person.label.demo.age.maxAge,
            subPopulationIndex = -1,
          )
        )
      }
    }

    logger.info(
      "Labeled ${results.size} inputs, ${results.map { it.vid }.distinct().size} unique VIDs"
    )
    return results
  }

  /**
   * Validates that every labeled VID falls within a [PopulationSpec] subpopulation range and that
   * the labeler-assigned demographics match the subpopulation's attributes.
   */
  fun validateAgainstPopulationSpec(
    results: List<LabeledVidResult>,
    populationSpec: PopulationSpec,
  ): List<LabeledVidResult> {
    return results.map { result ->
      val (subPopIndex, subPop) =
        populationSpec.subpopulationsList.withIndex().firstOrNull { (_, sub) ->
          sub.vidRangesList.any { range ->
            result.vid >= range.startVid && result.vid <= range.endVidInclusive
          }
        }
          ?: error(
            "VID ${result.vid} (from input ${result.inputId}) not in any PopulationSpec range"
          )

      result.copy(subPopulationIndex = subPopIndex)
    }
  }

  private fun <T : Message> buildTemplateFieldsByTypeUrl(
    messageInstance: T
  ): Map<String, FieldDescriptor> = buildMap {
    for (field in messageInstance.descriptorForType.fields) {
      if (field.type != FieldDescriptor.Type.MESSAGE) continue
      val typeUrl = ProtoReflection.getTypeUrl(field.messageType)
      put(typeUrl, field)
    }
  }

  private fun <T : Message> buildSubPopulationPrototypes(
    messageInstance: T,
    populationSpec: PopulationSpec,
    templateFieldsByTypeUrl: Map<String, FieldDescriptor>,
  ): Map<Int, T> = buildMap {
    for ((index, subPop) in populationSpec.subpopulationsList.withIndex()) {
      val builder = messageInstance.newBuilderForType()
      for (attribute in subPop.attributesList) {
        val templateField =
          templateFieldsByTypeUrl[attribute.typeUrl]
            ?: throw IllegalArgumentException(
              "Attribute type_url ${attribute.typeUrl} not in ${messageInstance.descriptorForType.fullName}"
            )
        builder.getFieldBuilder(templateField).mergeFrom(attribute.value)
      }
      @Suppress("UNCHECKED_CAST") put(index, builder.build() as T)
    }
  }

  private fun <T : Message> generateDayEvents(
    labeledVids: List<LabeledVidResult>,
    subPopulationPrototypes: Map<Int, T>,
    nonPopulationFieldValues: Map<String, FieldValue>,
    dateProgression: LocalDateProgression,
    date: LocalDate,
    frequency: Int,
    numDays: Int,
    zoneId: ZoneId,
  ): Sequence<LabeledEvent<T>> = sequence {
    val dayNumber = ChronoUnit.DAYS.between(dateProgression.start, date)

    for (result in labeledVids) {
      val prototype = subPopulationPrototypes[result.subPopulationIndex] ?: continue
      val builder = prototype.toBuilder()
      for ((path, fieldValue) in nonPopulationFieldValues) {
        setField(builder, path.split('.'), fieldValue)
      }
      @Suppress("UNCHECKED_CAST") val message = builder.build() as T

      for (i in 1..frequency) {
        val dayToLog =
          (FINGERPRINT_FUNCTION.hashLong(result.vid * i).asLong() % numDays + numDays) % numDays
        if (dayToLog == dayNumber) {
          val hashInput =
            result.vid
              .toByteString(ByteOrder.BIG_ENDIAN)
              .concat(dayToLog.toByteString(ByteOrder.BIG_ENDIAN))
          val hashValue =
            abs(Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong())
          val impressionTime = date.atStartOfDay(zoneId).plusSeconds(hashValue % SECONDS_PER_DAY)
          yield(LabeledEvent(impressionTime.toInstant(), result.vid, message))
        }
      }
    }
  }

  private fun setField(builder: Message.Builder, fieldPath: List<String>, fieldValue: FieldValue) {
    val field = builder.descriptorForType.findFieldByName(fieldPath.first()) ?: return

    if (fieldPath.size == 1) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      val value: Any =
        when (fieldValue.valueCase) {
          FieldValue.ValueCase.STRING_VALUE -> fieldValue.stringValue
          FieldValue.ValueCase.BOOL_VALUE -> fieldValue.boolValue
          FieldValue.ValueCase.ENUM_VALUE -> field.enumType.findValueByNumber(fieldValue.enumValue)
          FieldValue.ValueCase.DOUBLE_VALUE -> fieldValue.doubleValue
          FieldValue.ValueCase.FLOAT_VALUE -> fieldValue.floatValue
          FieldValue.ValueCase.INT32_VALUE -> fieldValue.int32Value
          FieldValue.ValueCase.INT64_VALUE -> fieldValue.int64Value
          FieldValue.ValueCase.DURATION_VALUE -> fieldValue.durationValue
          FieldValue.ValueCase.TIMESTAMP_VALUE -> fieldValue.timestampValue
          FieldValue.ValueCase.VALUE_NOT_SET -> throw IllegalArgumentException()
        }
      builder.setField(field, value)
      return
    }
    setField(builder.getFieldBuilder(field), fieldPath.drop(1), fieldValue)
  }

  private fun ReferenceVidEventGroupSpec.DateSpec.DateRange.toProgression(): LocalDateProgression {
    return start.toLocalDate()..endExclusive.toLocalDate().minusDays(1)
  }

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}
