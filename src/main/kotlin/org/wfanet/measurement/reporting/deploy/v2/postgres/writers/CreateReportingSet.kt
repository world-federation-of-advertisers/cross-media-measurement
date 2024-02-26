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
 */
class CreateReportingSet(private val request: CreateReportingSetRequest) :
  PostgresWriter<ReportingSet>() {
  private data class PrimitiveReportingSetBasesStatementComponents(
    val primitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit,
    val weightedSubsetUnionPrimitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit,
    val primitiveReportingSetBasisFiltersCurIndex: Int,
    val primitiveReportingSetBasisFiltersRowsSqlList: List<String>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(request.reportingSet.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val reportingSetId = idGenerator.generateInternalId()
    val externalReportingSetId: String = request.externalReportingSetId

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
          ReportingSetDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", externalReportingSetId)
        bind("$4", request.reportingSet.displayName)
        bind("$5", request.reportingSet.filter)
        bind("$6", request.reportingSet.details)
        bind("$7", request.reportingSet.details.toJson())
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
        val externalReportingSetIds: Set<String> =
          createExternalReportingSetIdsSet(request.reportingSet)

        // Map of external ReportingSet ID to internal ReportingSet ID.
        val reportingSetMap = mutableMapOf<String, InternalId>()

        ReportingSetReader(transactionContext)
          .readIds(measurementConsumerId, externalReportingSetIds)
          .collect { reportingSetMap[it.externalReportingSetId] = it.reportingSetId }

        val setExpressionId = idGenerator.generateInternalId()

        val setExpressionsStatement =
          boundStatement(
            """
          INSERT INTO SetExpressions (MeasurementConsumerId, ReportingSetId, SetExpressionId, Operation, LeftHandSetExpressionId, LeftHandReportingSetId, RightHandSetExpressionId, RightHandReportingSetId)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          """
          ) {
            addSetExpressionsBindings(
              this,
              setExpressionId = setExpressionId,
              measurementConsumerId = measurementConsumerId,
              reportingSetId = reportingSetId,
              request.reportingSet.composite,
              reportingSetMap,
            )
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
          reportingSetMap,
        )
      }
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        throw IllegalArgumentException()
      }
    }

    return request.reportingSet.copy {
      this.externalReportingSetId = externalReportingSetId
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

    val eventGroupsRowsSqlList = mutableListOf<String>()
    var eventGroupsCurIndex = 1
    val eventGroupsOffset = 4
    val eventGroupBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    cmmsEventGroupKeys.forEach {
      eventGroupMap.computeIfAbsent(
        ReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = it.cmmsDataProviderId
          cmmsEventGroupId = it.cmmsEventGroupId
        }
      ) {
        val id = idGenerator.generateInternalId()
        val tempEventGroupsCurIndex = eventGroupsCurIndex
        eventGroupBinders.add {
          bind("$${tempEventGroupsCurIndex}", measurementConsumerId)
          bind("$${tempEventGroupsCurIndex + 1}", id)
          bind("$${tempEventGroupsCurIndex + 2}", it.cmmsDataProviderId)
          bind("$${tempEventGroupsCurIndex + 3}", it.cmmsEventGroupId)
        }
        eventGroupsRowsSqlList.add(
          generateParameterizedInsertValues(
            eventGroupsCurIndex,
            eventGroupsCurIndex + eventGroupsOffset,
          )
        )
        eventGroupsCurIndex += eventGroupsOffset
        id
      }
    }

    val eventGroupsStatement: BoundStatement? =
      if (eventGroupBinders.size > 0) {
        boundStatement(
          """
          INSERT INTO EventGroups (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
          VALUES
          ${eventGroupsRowsSqlList.joinToString(",")}
          """
        ) {
          for (binder in eventGroupBinders) {
            binder()
          }
        }
      } else {
        null
      }

    val reportingSetEventGroupsRowsSqlList = mutableListOf<String>()
    var reportingSetEventGroupsCurIndex = 1
    val reportingSetEventGroupsOffset = 3
    val reportingSetEventGroupBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    eventGroupMap.values.forEach {
      val tempReportingSetEventGroupsCurIndex = reportingSetEventGroupsCurIndex
      reportingSetEventGroupBinders.add {
        bind("$${tempReportingSetEventGroupsCurIndex}", measurementConsumerId)
        bind("$${tempReportingSetEventGroupsCurIndex + 1}", reportingSetId)
        bind("$${tempReportingSetEventGroupsCurIndex + 2}", it)
      }
      reportingSetEventGroupsRowsSqlList.add(
        generateParameterizedInsertValues(
          reportingSetEventGroupsCurIndex,
          reportingSetEventGroupsCurIndex + reportingSetEventGroupsOffset,
        )
      )
      reportingSetEventGroupsCurIndex += reportingSetEventGroupsOffset
    }

    val reportingSetEventGroupsStatement =
      boundStatement(
        """
        INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
        VALUES
        ${reportingSetEventGroupsRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in reportingSetEventGroupBinders) {
          binder()
        }
      }

    transactionContext.run {
      if (eventGroupsStatement != null) {
        executeStatement(eventGroupsStatement)
      }
      executeStatement(reportingSetEventGroupsStatement)
    }
  }

  private fun createExternalReportingSetIdsSet(reportingSet: ReportingSet): Set<String> {
    val externalReportingIds = mutableSetOf<String>()
    externalReportingIds.addAll(reportingSet.composite.getExternalReportingSetIds())
    reportingSet.weightedSubsetUnionsList.forEach {
      externalReportingIds.addAll(it.getExternalReportingSetIds())
    }
    return externalReportingIds
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

  private fun TransactionScope.addSetExpressionsBindings(
    statementBuilder: BoundStatement.Builder,
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
        addSetExpressionsBindings(
          statementBuilder,
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
        leftHandReportingSetId =
          reportingSetMap[setExpression.lhs.externalReportingSetId]
            ?: throw ReportingSetNotFoundException()
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
        addSetExpressionsBindings(
          statementBuilder,
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
        rightHandReportingSetId =
          reportingSetMap[setExpression.rhs.externalReportingSetId]
            ?: throw ReportingSetNotFoundException()
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = null
      }
    }

    statementBuilder.addBinding {
      bind("$1", measurementConsumerId)
      bind("$2", reportingSetId)
      bind("$3", setExpressionId)
      bind("$4", setExpression.operationValue)
      bind("$5", leftHandSetExpressionId)
      bind("$6", leftHandReportingSetId)
      bind("$7", rightHandSetExpressionId)
      bind("$8", rightHandReportingSetId)
    }
  }

  private suspend fun TransactionScope.insertWeightedSubsetUnions(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnions: List<WeightedSubsetUnion>,
    reportingSetMap: Map<String, InternalId> = mapOf(),
  ) {
    var weightedSubsetUnionsCurIndex = 1
    val weightedSubsetUnionsOffset = 5
    val weightedSubsetUnionsRowsSqlList = mutableListOf<String>()
    val weightedSubsetUnionsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    var primitiveReportingSetBasesCurIndex = 1
    val primitiveReportingSetBasesOffset = 3
    val primitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    var weightedSubsetUnionPrimitiveReportingSetBasesCurIndex = 1
    val weightedSubsetUnionPrimitiveReportingSetBasesOffset = 4
    val weightedSubsetUnionPrimitiveReportingSetBasesRowsSqlList = mutableListOf<String>()
    val weightedSubsetUnionPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()

    var primitiveReportingSetBasisFiltersCurIndex = 1
    val primitiveReportingSetBasisFiltersOffset = 4
    val primitiveReportingSetBasisFiltersRowsSqlList = mutableListOf<String>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    weightedSubsetUnions.forEach { weightedSubsetUnion ->
      val weightedSubsetUnionId = idGenerator.generateInternalId()
      val tempWeightedSubsetUnionsCurIndex = weightedSubsetUnionsCurIndex
      weightedSubsetUnionsBinders.add {
        bind("$${tempWeightedSubsetUnionsCurIndex}", measurementConsumerId)
        bind("$${tempWeightedSubsetUnionsCurIndex + 1}", reportingSetId)
        bind("$${tempWeightedSubsetUnionsCurIndex + 2}", weightedSubsetUnionId)
        bind("$${tempWeightedSubsetUnionsCurIndex + 3}", weightedSubsetUnion.weight)
        bind("$${tempWeightedSubsetUnionsCurIndex + 4}", weightedSubsetUnion.binaryRepresentation)
      }
      weightedSubsetUnionsRowsSqlList.add(
        generateParameterizedInsertValues(
          weightedSubsetUnionsCurIndex,
          weightedSubsetUnionsCurIndex + weightedSubsetUnionsOffset,
        )
      )
      weightedSubsetUnionsCurIndex += weightedSubsetUnionsOffset

      weightedSubsetUnion.primitiveReportingSetBasesList.forEach {
        val statementComponents =
          createPrimitiveReportingSetBasisStatementComponents(
            measurementConsumerId = measurementConsumerId,
            reportingSetId = reportingSetId,
            weightedSubsetUnionId = weightedSubsetUnionId,
            primitiveReportingSetBasis = it,
            reportingSetMap = reportingSetMap,
            primitiveReportingSetBasesStartingIndex = primitiveReportingSetBasesCurIndex,
            weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex =
              weightedSubsetUnionPrimitiveReportingSetBasesCurIndex,
            primitiveReportingSetBasisFiltersStartingIndex =
              primitiveReportingSetBasisFiltersCurIndex,
            primitiveReportingSetBasisFiltersOffset = primitiveReportingSetBasisFiltersOffset,
          )

        primitiveReportingSetBasesBinders.add(statementComponents.primitiveReportingSetBasesBinder)
        primitiveReportingSetBasesRowsSqlList.add(
          generateParameterizedInsertValues(
            primitiveReportingSetBasesCurIndex,
            primitiveReportingSetBasesCurIndex + primitiveReportingSetBasesOffset,
          )
        )
        primitiveReportingSetBasesCurIndex += primitiveReportingSetBasesOffset

        weightedSubsetUnionPrimitiveReportingSetBasesBinders.add(
          statementComponents.weightedSubsetUnionPrimitiveReportingSetBasesBinder
        )
        weightedSubsetUnionPrimitiveReportingSetBasesRowsSqlList.add(
          generateParameterizedInsertValues(
            weightedSubsetUnionPrimitiveReportingSetBasesCurIndex,
            weightedSubsetUnionPrimitiveReportingSetBasesCurIndex +
              weightedSubsetUnionPrimitiveReportingSetBasesOffset,
          )
        )
        weightedSubsetUnionPrimitiveReportingSetBasesCurIndex +=
          weightedSubsetUnionPrimitiveReportingSetBasesOffset

        primitiveReportingSetBasisFiltersCurIndex =
          statementComponents.primitiveReportingSetBasisFiltersCurIndex
        primitiveReportingSetBasisFiltersRowsSqlList.addAll(
          statementComponents.primitiveReportingSetBasisFiltersRowsSqlList
        )
        primitiveReportingSetBasisFiltersBinders.addAll(
          statementComponents.primitiveReportingSetBasisFiltersBinders
        )
      }
    }

    val weightedSubsetUnionsStatement =
      boundStatement(
        """
        INSERT INTO WeightedSubsetUnions (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, Weight, BinaryRepresentation)
        VALUES
        ${weightedSubsetUnionsRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in weightedSubsetUnionsBinders) {
          binder()
        }
      }

    val primitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBases (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetId)
        VALUES
        ${primitiveReportingSetBasesRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in primitiveReportingSetBasesBinders) {
          binder()
        }
      }

    val weightedSubsetUnionPrimitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO WeightedSubsetUnionPrimitiveReportingSetBases (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
        VALUES
        ${weightedSubsetUnionPrimitiveReportingSetBasesRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in weightedSubsetUnionPrimitiveReportingSetBasesBinders) {
          binder()
        }
      }

    val primitiveReportingSetBasisFiltersStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBasisFilters (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetBasisFilterId, Filter)
        VALUES
        ${primitiveReportingSetBasisFiltersRowsSqlList.joinToString(",")}
        """
      ) {
        for (binder in primitiveReportingSetBasisFiltersBinders) {
          binder()
        }
      }

    transactionContext.run {
      executeStatement(weightedSubsetUnionsStatement)
      executeStatement(primitiveReportingSetBasesStatement)
      executeStatement(weightedSubsetUnionPrimitiveReportingSetBasesStatement)
      if (primitiveReportingSetBasisFiltersBinders.size > 0) {
        executeStatement(primitiveReportingSetBasisFiltersStatement)
      }
    }
  }

  private fun TransactionScope.createPrimitiveReportingSetBasisStatementComponents(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnionId: InternalId,
    primitiveReportingSetBasis: PrimitiveReportingSetBasis,
    reportingSetMap: Map<String, InternalId> = mapOf(),
    primitiveReportingSetBasesStartingIndex: Int,
    weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex: Int,
    primitiveReportingSetBasisFiltersStartingIndex: Int,
    primitiveReportingSetBasisFiltersOffset: Int,
  ): PrimitiveReportingSetBasesStatementComponents {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId()

    val primitiveReportingSetId =
      reportingSetMap[primitiveReportingSetBasis.externalReportingSetId]
        ?: throw ReportingSetNotFoundException()

    val primitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit = {
      bind("$${primitiveReportingSetBasesStartingIndex}", measurementConsumerId)
      bind("$${primitiveReportingSetBasesStartingIndex + 1}", primitiveReportingSetBasisId)
      bind("$${primitiveReportingSetBasesStartingIndex + 2}", primitiveReportingSetId)
    }

    val weightedSubsetUnionPrimitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit = {
      bind("$${weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex}", measurementConsumerId)
      bind("$${weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex + 1}", reportingSetId)
      bind(
        "$${weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex + 2}",
        weightedSubsetUnionId,
      )
      bind(
        "$${weightedSubsetUnionPrimitiveReportingSetBasesStartingIndex + 3}",
        primitiveReportingSetBasisId,
      )
    }

    val primitiveReportingSetBasisFiltersRowsSqlList = mutableListOf<String>()
    var primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersStartingIndex
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    primitiveReportingSetBasis.filtersList.forEach {
      val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()
      val tempPrimitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersCurIndex
      primitiveReportingSetBasisFiltersBinders.add {
        bind("$${tempPrimitiveReportingSetBasisFiltersCurIndex}", measurementConsumerId)
        bind("$${tempPrimitiveReportingSetBasisFiltersCurIndex + 1}", primitiveReportingSetBasisId)
        bind(
          "$${tempPrimitiveReportingSetBasisFiltersCurIndex + 2}",
          primitiveReportingSetBasisFilterId,
        )
        bind("$${tempPrimitiveReportingSetBasisFiltersCurIndex + 3}", it)
      }
      primitiveReportingSetBasisFiltersRowsSqlList.add(
        generateParameterizedInsertValues(
          primitiveReportingSetBasisFiltersCurIndex,
          primitiveReportingSetBasisFiltersCurIndex + primitiveReportingSetBasisFiltersOffset,
        )
      )
      primitiveReportingSetBasisFiltersCurIndex += primitiveReportingSetBasisFiltersOffset
    }

    return PrimitiveReportingSetBasesStatementComponents(
      primitiveReportingSetBasesBinder = primitiveReportingSetBasesBinder,
      weightedSubsetUnionPrimitiveReportingSetBasesBinder =
        weightedSubsetUnionPrimitiveReportingSetBasesBinder,
      primitiveReportingSetBasisFiltersBinders = primitiveReportingSetBasisFiltersBinders,
      primitiveReportingSetBasisFiltersRowsSqlList = primitiveReportingSetBasisFiltersRowsSqlList,
      primitiveReportingSetBasisFiltersCurIndex = primitiveReportingSetBasisFiltersCurIndex,
    )
  }
}
