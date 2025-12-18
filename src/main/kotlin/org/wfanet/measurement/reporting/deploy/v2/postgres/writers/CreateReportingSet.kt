/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import io.r2dbc.postgresql.api.PostgresqlException
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.PrimitiveReportingSetBasis
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.SetExpression
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.WeightedSubsetUnion
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.EventGroupReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

const val INTEGRITY_CONSTRAINT_VIOLATION = "23505"

/**
 * Inserts a Reporting Set into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [ReportingSetAlreadyExistsException] ReportingSet already exists
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [CampaignGroupInvalidException] Existing ReportingSet is not a valid Campaign Group
 */
class CreateReportingSet(private val request: CreateReportingSetRequest) :
  PostgresWriter<ReportingSet>() {
  private data class SetExpressionsValues(
    val setExpressionId: InternalId,
    val operationValue: Int,
    val leftHandSetExpressionId: InternalId?,
    val leftHandReportingSetId: InternalId?,
    val rightHandSetExpressionId: InternalId?,
    val rightHandReportingSetId: InternalId?,
  )

  private data class WeightedSubsetUnionsValues(
    val weightedSubsetUnionId: InternalId,
    val weight: Int,
    val binaryRepresentation: Int,
  )

  private data class PrimitiveReportingSetBasesInsertData(
    val primitiveReportingSetBasesValues: PrimitiveReportingSetBasesValues,
    val weightedSubsetUnionPrimitiveReportingSetBasesValues:
      WeightedSubsetUnionPrimitiveReportingSetBasesValues,
    val primitiveReportingSetBasisFiltersValuesList: List<PrimitiveReportingSetBasisFiltersValues>,
  )

  private data class PrimitiveReportingSetBasesValues(
    val primitiveReportingSetBasisId: InternalId,
    val primitiveReportingSetId: InternalId,
  )

  private data class WeightedSubsetUnionPrimitiveReportingSetBasesValues(
    val weightedSubsetUnionId: InternalId,
    val primitiveReportingSetBasisId: InternalId,
  )

  private data class PrimitiveReportingSetBasisFiltersValues(
    val primitiveReportingSetBasisId: InternalId,
    val primitiveReportingSetBasisFilterId: InternalId,
    val filter: String,
  )

  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val cmmsMeasurementConsumerId = request.reportingSet.cmmsMeasurementConsumerId
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext).getByCmmsId(cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(cmmsMeasurementConsumerId))
        .measurementConsumerId

    val reportingSetId = idGenerator.generateInternalId()
    val externalReportingSetId: String = request.externalReportingSetId
    val externalCampaignGroupId = request.reportingSet.externalCampaignGroupId
    val campaignGroupIds: ReportingSetReader.ReportingSetIds? =
      if (externalCampaignGroupId.isNotEmpty()) {
        if (externalCampaignGroupId == externalReportingSetId) {
          ReportingSetReader.ReportingSetIds(
            measurementConsumerId,
            reportingSetId,
            externalReportingSetId,
          )
        } else {
          ReportingSetReader(transactionContext)
            .readCampaignGroup(measurementConsumerId, externalCampaignGroupId)
            ?: throw ReportingSetNotFoundException(
              cmmsMeasurementConsumerId,
              externalCampaignGroupId,
            )
        }
      } else {
        null
      }

    val statement =
      boundStatement(
        """
      INSERT INTO ReportingSets
        (
          MeasurementConsumerId,
          ReportingSetId,
          ExternalReportingSetId,
          DisplayName,
          Filter,
          ReportingSetDetails,
          ReportingSetDetailsJson,
          CampaignGroupId
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", externalReportingSetId)
        bind("$4", request.reportingSet.displayName)
        bind("$5", request.reportingSet.filter)
        bind("$6", request.reportingSet.details)
        bind("$7", request.reportingSet.details.toJson())
        bind("$8", campaignGroupIds?.reportingSetId)
      }

    try {
      transactionContext.executeStatement(statement)
    } catch (e: R2dbcDataIntegrityViolationException) {
      if (e is PostgresqlException && e.errorDetails.code == INTEGRITY_CONSTRAINT_VIOLATION) {
        throw ReportingSetAlreadyExistsException()
      } else {
        throw e
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (request.reportingSet.valueCase) {
      ReportingSet.ValueCase.PRIMITIVE -> {
        insertReportingSetEventGroups(
          measurementConsumerId,
          reportingSetId,
          request.reportingSet.primitive.eventGroupKeysList,
        )
      }
      ReportingSet.ValueCase.COMPOSITE -> {
        val externalReportingSetIds: Set<String> = getExternalReportingSetIds(request.reportingSet)
        val reportingSetIdByExternalId = buildMap {
          ReportingSetReader(transactionContext)
            .readIds(measurementConsumerId, externalReportingSetIds)
            .collect { put(it.externalReportingSetId, it.reportingSetId) }
        }
        for (externalReportingSetId in externalReportingSetIds) {
          if (!reportingSetIdByExternalId.containsKey(externalReportingSetId)) {
            throw ReportingSetNotFoundException(cmmsMeasurementConsumerId, externalReportingSetId)
          }
        }

        val setExpressionId = idGenerator.generateInternalId()
        val setExpressionsValuesList: List<SetExpressionsValues> = buildList {
          createSetExpressionsValues(
            this,
            setExpressionId = setExpressionId,
            measurementConsumerId = measurementConsumerId,
            reportingSetId = reportingSetId,
            request.reportingSet.composite,
            reportingSetIdByExternalId,
          )
        }

        val setExpressionsStatement =
          valuesListBoundStatement(
            valuesStartIndex = 0,
            paramCount = 8,
            """
          INSERT INTO SetExpressions (
            MeasurementConsumerId,
            ReportingSetId,
            SetExpressionId,
            Operation,
            LeftHandSetExpressionId,
            LeftHandReportingSetId,
            RightHandSetExpressionId,
            RightHandReportingSetId
          )
          VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
          """,
          ) {
            setExpressionsValuesList.forEach {
              addValuesBinding {
                bindValuesParam(0, measurementConsumerId)
                bindValuesParam(1, reportingSetId)
                bindValuesParam(2, it.setExpressionId)
                bindValuesParam(3, it.operationValue)
                bindValuesParam(4, it.leftHandSetExpressionId)
                bindValuesParam(5, it.leftHandReportingSetId)
                bindValuesParam(6, it.rightHandSetExpressionId)
                bindValuesParam(7, it.rightHandReportingSetId)
              }
            }
          }
        transactionContext.executeStatement(setExpressionsStatement)

        val updateReportingSetStatement =
          boundStatement(
            """
            UPDATE ReportingSets SET SetExpressionId = $1
              WHERE MeasurementConsumerId = $2 AND ReportingSetId = $3
            """
          ) {
            bind("$1", setExpressionId)
            bind("$2", measurementConsumerId)
            bind("$3", reportingSetId)
          }

        transactionContext.executeStatement(updateReportingSetStatement)

        insertWeightedSubsetUnions(
          measurementConsumerId,
          reportingSetId,
          request.reportingSet.weightedSubsetUnionsList,
          reportingSetIdByExternalId,
        )
      }
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        throw IllegalArgumentException()
      }
    }

    return request.reportingSet.copy {
      this.externalReportingSetId = externalReportingSetId
      this.externalCampaignGroupId = externalCampaignGroupId
      if (this.valueCase == ReportingSet.ValueCase.PRIMITIVE && weightedSubsetUnions.isEmpty()) {
        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            weight = 1
            binaryRepresentation = 1
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                this.externalReportingSetId = externalReportingSetId
                if (request.reportingSet.filter.isNotBlank()) {
                  filters += request.reportingSet.filter
                }
              }
          }
      }
    }
  }

  private suspend fun TransactionScope.insertReportingSetEventGroups(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    eventGroups: List<ReportingSet.Primitive.EventGroupKey>,
  ) {
    // Map of Primitive Reporting Set EventGroupKey to internal EventGroup ID.
    val eventGroupMap = mutableMapOf<ReportingSet.Primitive.EventGroupKey, InternalId>()

    val cmmsEventGroupKeys: Collection<EventGroupReader.CmmsEventGroupKey> =
      eventGroups.distinct().map {
        EventGroupReader.CmmsEventGroupKey(
          cmmsDataProviderId = it.cmmsDataProviderId,
          cmmsEventGroupId = it.cmmsEventGroupId,
        )
      }

    EventGroupReader(transactionContext).getByCmmsEventGroupKey(cmmsEventGroupKeys).collect {
      eventGroupMap[
        ReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = it.cmmsDataProviderId
          cmmsEventGroupId = it.cmmsEventGroupId
        }] = it.eventGroupId
    }

    val eventGroupBinders =
      mutableListOf<ValuesListBoundStatement.ValuesListBoundStatementBuilder.() -> Unit>()

    cmmsEventGroupKeys.forEach {
      eventGroupMap.computeIfAbsent(
        ReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = it.cmmsDataProviderId
          cmmsEventGroupId = it.cmmsEventGroupId
        }
      ) {
        val id = idGenerator.generateInternalId()
        eventGroupBinders.add {
          bindValuesParam(0, measurementConsumerId)
          bindValuesParam(1, id)
          bindValuesParam(2, it.cmmsDataProviderId)
          bindValuesParam(3, it.cmmsEventGroupId)
        }
        id
      }
    }

    val eventGroupsStatement: BoundStatement? =
      if (eventGroupBinders.size > 0) {
        valuesListBoundStatement(
          valuesStartIndex = 0,
          paramCount = 4,
          """
              INSERT INTO EventGroups (
                MeasurementConsumerId,
                EventGroupId,
                CmmsDataProviderId,
                CmmsEventGroupId
              )
              VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER
              }
              """,
        ) {
          eventGroupBinders.forEach { addValuesBinding(it) }
        }
      } else {
        null
      }

    val reportingSetEventGroupsStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 3,
        """
            INSERT INTO ReportingSetEventGroups (
              MeasurementConsumerId,
              ReportingSetId,
              EventGroupId
            )
            VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
            """,
      ) {
        eventGroupMap.values.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, reportingSetId)
            bindValuesParam(2, it)
          }
        }
      }

    transactionContext.run {
      if (eventGroupsStatement != null) {
        executeStatement(eventGroupsStatement)
      }
      executeStatement(reportingSetEventGroupsStatement)
    }
  }

  /**
   * Returns the [Set] of external ReportingSet IDs referenced by the specified composite
   * [ReportingSet].
   */
  private fun getExternalReportingSetIds(reportingSet: ReportingSet): Set<String> {
    require(reportingSet.hasComposite())

    return buildSet {
      addAll(reportingSet.composite.getExternalReportingSetIds())
      reportingSet.weightedSubsetUnionsList.forEach { addAll(it.getExternalReportingSetIds()) }
    }
  }

  private fun SetExpression.getExternalReportingSetIds(): Set<String> {
    val externalReportingSetIds = mutableSetOf<String>()
    val source = this
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        externalReportingSetIds.addAll(source.lhs.expression.getExternalReportingSetIds())
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        externalReportingSetIds.add(source.lhs.externalReportingSetId)
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (this.rhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        externalReportingSetIds.addAll(source.rhs.expression.getExternalReportingSetIds())
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        externalReportingSetIds.add(source.rhs.externalReportingSetId)
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
    }

    return externalReportingSetIds
  }

  private fun WeightedSubsetUnion.getExternalReportingSetIds(): Set<String> {
    return primitiveReportingSetBasesList.asSequence().map { it.externalReportingSetId }.toSet()
  }

  private fun TransactionScope.createSetExpressionsValues(
    values: MutableList<SetExpressionsValues>,
    setExpressionId: InternalId,
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    setExpression: SetExpression,
    reportingSetMap: Map<String, InternalId> = mapOf(),
  ) {
    val leftHandSetExpressionId: InternalId?
    val leftHandReportingSetId: InternalId?
    val rightHandSetExpressionId: InternalId?
    val rightHandReportingSetId: InternalId?

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setExpression.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        leftHandSetExpressionId = idGenerator.generateInternalId()
        createSetExpressionsValues(
          values,
          leftHandSetExpressionId,
          measurementConsumerId,
          reportingSetId,
          setExpression.lhs.expression,
          reportingSetMap,
        )
        leftHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        leftHandSetExpressionId = null
        leftHandReportingSetId = reportingSetMap.getValue(setExpression.lhs.externalReportingSetId)
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        leftHandSetExpressionId = null
        leftHandReportingSetId = null
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setExpression.rhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        rightHandSetExpressionId = idGenerator.generateInternalId()
        createSetExpressionsValues(
          values,
          rightHandSetExpressionId,
          measurementConsumerId,
          reportingSetId,
          setExpression.rhs.expression,
          reportingSetMap,
        )
        rightHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = reportingSetMap.getValue(setExpression.rhs.externalReportingSetId)
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = null
      }
    }

    values.add(
      SetExpressionsValues(
        setExpressionId = setExpressionId,
        operationValue = setExpression.operationValue,
        leftHandSetExpressionId = leftHandSetExpressionId,
        leftHandReportingSetId = leftHandReportingSetId,
        rightHandSetExpressionId = rightHandSetExpressionId,
        rightHandReportingSetId = rightHandReportingSetId,
      )
    )
  }

  private suspend fun TransactionScope.insertWeightedSubsetUnions(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnions: List<WeightedSubsetUnion>,
    reportingSetMap: Map<String, InternalId> = mapOf(),
  ) {
    val weightedSubsetUnionsValuesList = mutableListOf<WeightedSubsetUnionsValues>()
    val primitiveReportingSetBasesValuesList = mutableListOf<PrimitiveReportingSetBasesValues>()
    val weightedSubsetUnionPrimitiveReportingSetBasesValuesList =
      mutableListOf<WeightedSubsetUnionPrimitiveReportingSetBasesValues>()
    val primitiveReportingSetBasisFiltersValuesList =
      mutableListOf<PrimitiveReportingSetBasisFiltersValues>()

    weightedSubsetUnions.forEach { weightedSubsetUnion ->
      val weightedSubsetUnionId = idGenerator.generateInternalId()
      weightedSubsetUnionsValuesList.add(
        WeightedSubsetUnionsValues(
          weightedSubsetUnionId = weightedSubsetUnionId,
          weight = weightedSubsetUnion.weight,
          binaryRepresentation = weightedSubsetUnion.binaryRepresentation,
        )
      )

      weightedSubsetUnion.primitiveReportingSetBasesList.forEach {
        val insertData =
          createPrimitiveReportingSetBasisInsertData(weightedSubsetUnionId, it, reportingSetMap)
        primitiveReportingSetBasesValuesList.add(insertData.primitiveReportingSetBasesValues)
        weightedSubsetUnionPrimitiveReportingSetBasesValuesList.add(
          insertData.weightedSubsetUnionPrimitiveReportingSetBasesValues
        )
        primitiveReportingSetBasisFiltersValuesList.addAll(
          insertData.primitiveReportingSetBasisFiltersValuesList
        )
      }
    }

    val weightedSubsetUnionsStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 5,
        """
        INSERT INTO WeightedSubsetUnions (
          MeasurementConsumerId,
          ReportingSetId,
          WeightedSubsetUnionId,
          Weight,
          BinaryRepresentation
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        weightedSubsetUnionsValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, reportingSetId)
            bindValuesParam(2, it.weightedSubsetUnionId)
            bindValuesParam(3, it.weight)
            bindValuesParam(4, it.binaryRepresentation)
          }
        }
      }

    val primitiveReportingSetBasesStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 3,
        """
        INSERT INTO PrimitiveReportingSetBases (
          MeasurementConsumerId,
          PrimitiveReportingSetBasisId,
          PrimitiveReportingSetId
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        primitiveReportingSetBasesValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.primitiveReportingSetBasisId)
            bindValuesParam(2, it.primitiveReportingSetId)
          }
        }
      }

    val weightedSubsetUnionPrimitiveReportingSetBasesStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 4,
        """
        INSERT INTO WeightedSubsetUnionPrimitiveReportingSetBases (
          MeasurementConsumerId,
          ReportingSetId,
          WeightedSubsetUnionId,
          PrimitiveReportingSetBasisId
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        weightedSubsetUnionPrimitiveReportingSetBasesValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, reportingSetId)
            bindValuesParam(2, it.weightedSubsetUnionId)
            bindValuesParam(3, it.primitiveReportingSetBasisId)
          }
        }
      }

    val primitiveReportingSetBasisFiltersStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 4,
        """
        INSERT INTO PrimitiveReportingSetBasisFilters (
          MeasurementConsumerId,
          PrimitiveReportingSetBasisId,
          PrimitiveReportingSetBasisFilterId,
          Filter
        )
        VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
        """,
      ) {
        primitiveReportingSetBasisFiltersValuesList.forEach {
          addValuesBinding {
            bindValuesParam(0, measurementConsumerId)
            bindValuesParam(1, it.primitiveReportingSetBasisId)
            bindValuesParam(2, it.primitiveReportingSetBasisFilterId)
            bindValuesParam(3, it.filter)
          }
        }
      }

    transactionContext.run {
      executeStatement(weightedSubsetUnionsStatement)
      executeStatement(primitiveReportingSetBasesStatement)
      executeStatement(weightedSubsetUnionPrimitiveReportingSetBasesStatement)
      if (primitiveReportingSetBasisFiltersValuesList.size > 0) {
        executeStatement(primitiveReportingSetBasisFiltersStatement)
      }
    }
  }

  private fun TransactionScope.createPrimitiveReportingSetBasisInsertData(
    weightedSubsetUnionId: InternalId,
    primitiveReportingSetBasis: PrimitiveReportingSetBasis,
    reportingSetMap: Map<String, InternalId> = mapOf(),
  ): PrimitiveReportingSetBasesInsertData {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId()

    val primitiveReportingSetId =
      reportingSetMap.getValue(primitiveReportingSetBasis.externalReportingSetId)

    val primitiveReportingSetBasesValues =
      PrimitiveReportingSetBasesValues(
        primitiveReportingSetBasisId = primitiveReportingSetBasisId,
        primitiveReportingSetId = primitiveReportingSetId,
      )

    val weightedSubsetUnionPrimitiveReportingSetBasesValues =
      WeightedSubsetUnionPrimitiveReportingSetBasesValues(
        weightedSubsetUnionId = weightedSubsetUnionId,
        primitiveReportingSetBasisId = primitiveReportingSetBasisId,
      )

    val primitiveReportingSetBasisFiltersValuesList =
      mutableListOf<PrimitiveReportingSetBasisFiltersValues>()
    primitiveReportingSetBasis.filtersList.forEach { filter ->
      val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
      primitiveReportingSetBasisFiltersValuesList.add(
        PrimitiveReportingSetBasisFiltersValues(
          primitiveReportingSetBasisId = primitiveReportingSetBasisId,
          primitiveReportingSetBasisFilterId = primitiveReportingSetBasisFilterId,
          filter = filter,
        )
      )
    }

    return PrimitiveReportingSetBasesInsertData(
      primitiveReportingSetBasesValues = primitiveReportingSetBasesValues,
      weightedSubsetUnionPrimitiveReportingSetBasesValues =
        weightedSubsetUnionPrimitiveReportingSetBasesValues,
      primitiveReportingSetBasisFiltersValuesList,
    )
  }
}
