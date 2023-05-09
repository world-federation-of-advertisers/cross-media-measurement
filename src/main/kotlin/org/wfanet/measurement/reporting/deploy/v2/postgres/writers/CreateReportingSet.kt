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

import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
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
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Reporting Set into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateReportingSet(private val reportingSet: ReportingSet) : PostgresWriter<ReportingSet>() {
  private data class PrimitiveReportingSetBasesBinders(
    val primitiveReportingSetBasesBinders: List<BoundStatement.Binder.() -> Unit>,
    val weightedSubsetUnionPrimitiveReportingSetBasesBinders:
      List<BoundStatement.Binder.() -> Unit>,
    val primitiveReportingSetBasisFiltersBinders: List<BoundStatement.Binder.() -> Unit>,
  )

  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(reportingSet.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val reportingSetId = idGenerator.generateInternalId()
    val externalReportingSetId = idGenerator.generateExternalId()

    val statement =
      boundStatement(
        """
      INSERT INTO ReportingSets (MeasurementConsumerId, ReportingSetId, ExternalReportingSetId, DisplayName, Filter)
        VALUES ($1, $2, $3, $4, $5)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", externalReportingSetId)
        bind("$4", reportingSet.displayName)
        bind("$5", reportingSet.filter)
      }

    transactionContext.executeStatement(statement)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (reportingSet.valueCase) {
      ReportingSet.ValueCase.PRIMITIVE -> {
        insertReportingSetEventGroups(
          measurementConsumerId,
          reportingSetId,
          reportingSet.primitive.eventGroupKeysList
        )
      }
      ReportingSet.ValueCase.COMPOSITE -> {
        val externalReportingSetIds: Set<ExternalId> =
          createExternalReportingSetIdsSet(reportingSet)

        // Map of external ReportingSet ID to internal ReportingSet ID.
        val reportingSetMap = mutableMapOf<ExternalId, InternalId>()

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
              reportingSet.composite,
              reportingSetMap
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
          reportingSet.weightedSubsetUnionsList,
          reportingSetMap
        )
      }
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        throw IllegalArgumentException()
      }
    }

    return reportingSet.copy {
      this.externalReportingSetId = externalReportingSetId.value
      if (this.valueCase == ReportingSet.ValueCase.PRIMITIVE && weightedSubsetUnions.isEmpty()) {
        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            weight = 1
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                this.externalReportingSetId = externalReportingSetId.value

                if (reportingSet.filter.isNotBlank()) {
                  filters += reportingSet.filter
                }
              }
          }
      }
    }
  }

  private suspend fun TransactionScope.insertReportingSetEventGroups(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    eventGroups: List<ReportingSet.Primitive.EventGroupKey>
  ) {
    // Map of Primitive Reporting Set EventGroupKey to internal EventGroup ID.
    val eventGroupMap = mutableMapOf<ReportingSet.Primitive.EventGroupKey, InternalId>()

    val cmmsEventGroupKeys: Collection<EventGroupReader.CmmsEventGroupKey> =
      eventGroups.distinct().map {
        EventGroupReader.CmmsEventGroupKey(
          cmmsDataProviderId = it.cmmsDataProviderId,
          cmmsEventGroupId = it.cmmsEventGroupId
        )
      }

    EventGroupReader(transactionContext).getByCmmsEventGroupKey(cmmsEventGroupKeys).collect {
      eventGroupMap[
        ReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = it.cmmsDataProviderId
          cmmsEventGroupId = it.cmmsEventGroupId
        }] = it.eventGroupId
    }

    val eventGroupBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    cmmsEventGroupKeys.forEach {
      eventGroupMap.computeIfAbsent(
        ReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = it.cmmsDataProviderId
          cmmsEventGroupId = it.cmmsEventGroupId
        }
      ) {
        val id = idGenerator.generateInternalId()
        eventGroupBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", id)
          bind("$3", it.cmmsDataProviderId)
          bind("$4", it.cmmsEventGroupId)
        }
        id
      }
    }

    val eventGroupsStatement: BoundStatement? =
      if (eventGroupBinders.size > 0) {
        boundStatement(
          """
              INSERT INTO EventGroups (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
              VALUES ($1, $2, $3, $4)
              """
        ) {
          eventGroupBinders.forEach { addBinding(it) }
        }
      } else {
        null
      }

    val reportingSetEventGroupsStatement =
      boundStatement(
        """
            INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
            VALUES ($1, $2, $3)
            """
      ) {
        eventGroupMap.values.forEach {
          addBinding {
            bind("$1", measurementConsumerId)
            bind("$2", reportingSetId)
            bind("$3", it)
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

  private fun createExternalReportingSetIdsSet(reportingSet: ReportingSet): Set<ExternalId> {
    val externalReportingIds = mutableSetOf<ExternalId>()
    externalReportingIds.addAll(reportingSet.composite.getExternalReportingSetIds())
    reportingSet.weightedSubsetUnionsList.forEach {
      externalReportingIds.addAll(it.getExternalReportingSetIds())
    }
    return externalReportingIds
  }

  private fun SetExpression.getExternalReportingSetIds(): Set<ExternalId> {
    val externalReportingSetIds = mutableSetOf<ExternalId>()
    val source = this
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        externalReportingSetIds.addAll(source.lhs.expression.getExternalReportingSetIds())
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        externalReportingSetIds.add(ExternalId(source.lhs.externalReportingSetId))
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (this.rhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        externalReportingSetIds.addAll(source.rhs.expression.getExternalReportingSetIds())
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        externalReportingSetIds.add(ExternalId(source.rhs.externalReportingSetId))
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
    }

    return externalReportingSetIds
  }

  private fun WeightedSubsetUnion.getExternalReportingSetIds(): Set<ExternalId> {
    val externalReportingSetIds = mutableSetOf<ExternalId>()
    this.primitiveReportingSetBasesList.forEach {
      externalReportingSetIds.add(ExternalId(it.externalReportingSetId))
    }
    return externalReportingSetIds
  }

  private fun TransactionScope.addSetExpressionsBindings(
    statementBuilder: BoundStatement.Builder,
    setExpressionId: InternalId,
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    setExpression: SetExpression,
    reportingSetMap: Map<ExternalId, InternalId> = mapOf()
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
          reportingSetMap
        )
        leftHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        leftHandSetExpressionId = null
        leftHandReportingSetId =
          reportingSetMap[ExternalId(setExpression.lhs.externalReportingSetId)]
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
          reportingSetMap
        )
        rightHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId =
          reportingSetMap[ExternalId(setExpression.rhs.externalReportingSetId)]
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
    reportingSetMap: Map<ExternalId, InternalId> = mapOf()
  ) {
    val weightedSubsetUnionsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val weightedSubsetUnionPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    weightedSubsetUnions.forEach { weightedSubsetUnion ->
      val weightedSubsetUnionId = idGenerator.generateInternalId()
      weightedSubsetUnionsBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", weightedSubsetUnionId)
        bind("$4", weightedSubsetUnion.weight)
      }

      weightedSubsetUnion.primitiveReportingSetBasesList.forEach {
        val binders =
          createPrimitiveReportingSetBasisBindings(
            measurementConsumerId,
            reportingSetId,
            weightedSubsetUnionId,
            it,
            reportingSetMap
          )
        primitiveReportingSetBasesBinders.addAll(binders.primitiveReportingSetBasesBinders)
        weightedSubsetUnionPrimitiveReportingSetBasesBinders.addAll(
          binders.weightedSubsetUnionPrimitiveReportingSetBasesBinders
        )
        primitiveReportingSetBasisFiltersBinders.addAll(
          binders.primitiveReportingSetBasisFiltersBinders
        )
      }
    }

    val weightedSubsetUnionsStatement =
      boundStatement(
        """
        INSERT INTO WeightedSubsetUnions (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, Weight)
        VALUES ($1, $2, $3, $4)
        """
      ) {
        weightedSubsetUnionsBinders.forEach { addBinding(it) }
      }

    val primitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBases (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetId)
        VALUES ($1, $2, $3)
        """
      ) {
        primitiveReportingSetBasesBinders.forEach { addBinding(it) }
      }

    val weightedSubsetUnionPrimitiveReportingSetBasesStatement =
      boundStatement(
        """
        INSERT INTO WeightedSubsetUnionPrimitiveReportingSetBases (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
        VALUES ($1, $2, $3, $4)
        """
      ) {
        weightedSubsetUnionPrimitiveReportingSetBasesBinders.forEach { addBinding(it) }
      }

    val primitiveReportingSetBasisFiltersStatement =
      boundStatement(
        """
        INSERT INTO PrimitiveReportingSetBasisFilters (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetBasisFilterId, Filter)
        VALUES ($1, $2, $3, $4)
        """
      ) {
        primitiveReportingSetBasisFiltersBinders.forEach { addBinding(it) }
      }

    transactionContext.run {
      executeStatement(weightedSubsetUnionsStatement)
      executeStatement(primitiveReportingSetBasesStatement)
      executeStatement(weightedSubsetUnionPrimitiveReportingSetBasesStatement)
      executeStatement(primitiveReportingSetBasisFiltersStatement)
    }
  }

  private fun TransactionScope.createPrimitiveReportingSetBasisBindings(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnionId: InternalId,
    primitiveReportingSetBasis: PrimitiveReportingSetBasis,
    reportingSetMap: Map<ExternalId, InternalId> = mapOf()
  ): PrimitiveReportingSetBasesBinders {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId()

    val primitiveReportingSetId =
      reportingSetMap[ExternalId(primitiveReportingSetBasis.externalReportingSetId)]
        ?: throw ReportingSetNotFoundException()

    val primitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit = {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetId)
    }

    val weightedSubsetUnionPrimitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit = {
      bind("$1", measurementConsumerId)
      bind("$2", reportingSetId)
      bind("$3", weightedSubsetUnionId)
      bind("$4", primitiveReportingSetBasisId)
    }

    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    primitiveReportingSetBasis.filtersList.forEach {
      primitiveReportingSetBasisFiltersBinders.add(
        insertPrimitiveReportingSetBasisFilter(
          measurementConsumerId,
          primitiveReportingSetBasisId,
          it
        )
      )
    }

    return PrimitiveReportingSetBasesBinders(
      primitiveReportingSetBasesBinders = listOf(primitiveReportingSetBasesBinder),
      weightedSubsetUnionPrimitiveReportingSetBasesBinders =
        listOf(weightedSubsetUnionPrimitiveReportingSetBasesBinder),
      primitiveReportingSetBasisFiltersBinders
    )
  }

  private fun TransactionScope.insertPrimitiveReportingSetBasisFilter(
    measurementConsumerId: InternalId,
    primitiveReportingSetBasisId: InternalId,
    filter: String
  ): BoundStatement.Binder.() -> Unit {
    val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()

    return {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetBasisFilterId)
      bind("$4", filter)
    }
  }
}
