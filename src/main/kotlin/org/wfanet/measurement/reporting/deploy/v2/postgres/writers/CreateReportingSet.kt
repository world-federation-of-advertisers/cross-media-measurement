// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.PrimitiveReportingSetBasis
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.SetExpression
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.WeightedSubsetUnion
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.postgres.writers.PostgresWriter
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
  private data class SetExpressionsBindersAndId(
    val setExpressionsBinders: List<BoundStatement.Binder.() -> Unit>,
    val setExpressionId: Long,
  )
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

    val reportingSetId = idGenerator.generateInternalId().value
    val externalReportingSetId = idGenerator.generateExternalId().value

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

    if (reportingSet.valueCase.equals(ReportingSet.ValueCase.PRIMITIVE)) {
      insertReportingSetEventGroups(
        measurementConsumerId,
        reportingSetId,
        reportingSet.primitive.eventGroupKeysList
      )
    } else if (reportingSet.valueCase.equals(ReportingSet.ValueCase.COMPOSITE)) {
      val reportingSetReader = ReportingSetReader(transactionContext)
      insertSetExpressions(
        reportingSetReader,
        measurementConsumerId,
        reportingSetId,
        reportingSet.composite,
        true
      )
      insertWeightedSubsetUnions(
        measurementConsumerId,
        reportingSetId,
        reportingSet.weightedSubsetUnionsList
      )
    }

    return reportingSet.copy { this.externalReportingSetId = externalReportingSetId }
  }

  private suspend fun TransactionScope.insertReportingSetEventGroups(
    measurementConsumerId: Long,
    reportingSetId: Long,
    eventGroups: List<ReportingSet.Primitive.EventGroupKey>
  ) {
    val eventGroupBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val reportingSetEventGroupsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    eventGroups.forEach {
      val eventGroupResult: EventGroupReader.Result? =
        EventGroupReader(transactionContext)
          .getByCmmsIds(
            cmmsDataProviderId = it.cmmsDataProviderId,
            cmmsEventGroupId = it.cmmsEventGroupId
          )

      val eventGroupId: Long
      if (eventGroupResult == null) {
        eventGroupId = idGenerator.generateInternalId().value
        eventGroupBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", eventGroupId)
          bind("$3", it.cmmsDataProviderId)
          bind("$4", it.cmmsEventGroupId)
        }
      } else {
        eventGroupId = eventGroupResult.eventGroupId
      }

      reportingSetEventGroupsBinders.add {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", eventGroupId)
      }
    }

    val eventGroupsStatement =
      boundStatement(
        """
              INSERT INTO EventGroups (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
              VALUES ($1, $2, $3, $4)
              """
      ) {
        eventGroupBinders.forEach { addBinding(it) }
      }

    val reportingSetEventGroupsStatement =
      boundStatement(
        """
            INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
            VALUES ($1, $2, $3)
            """
      ) {
        reportingSetEventGroupsBinders.forEach { addBinding(it) }
      }

    transactionContext.run {
      executeStatement(eventGroupsStatement)
      executeStatement(reportingSetEventGroupsStatement)
    }
  }

  private suspend fun TransactionScope.insertSetExpressions(
    reportingSetReader: ReportingSetReader,
    measurementConsumerId: Long,
    reportingSetId: Long,
    setExpression: SetExpression,
    root: Boolean
  ): SetExpressionsBindersAndId {
    val setExpressionId = idGenerator.generateInternalId().value

    val leftHandSetExpressionId: Long?
    val leftHandReportingSetId: Long?
    val rightHandSetExpressionId: Long?
    val rightHandReportingSetId: Long?

    val setExpressionsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setExpression.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        val setExpressionsBindersAndId =
          insertSetExpressions(
            reportingSetReader,
            measurementConsumerId,
            reportingSetId,
            setExpression.lhs.expression,
            false
          )
        setExpressionsBinders.addAll(setExpressionsBindersAndId.setExpressionsBinders)
        leftHandSetExpressionId = setExpressionsBindersAndId.setExpressionId
        leftHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        leftHandSetExpressionId = null
        val idResult =
          reportingSetReader.readId(measurementConsumerId, setExpression.lhs.externalReportingSetId)
            ?: throw ReportingSetNotFoundException()
        leftHandReportingSetId = idResult.reportingSetId
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        leftHandSetExpressionId = null
        leftHandReportingSetId = null
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setExpression.rhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        val setExpressionsBindersAndId =
          insertSetExpressions(
            reportingSetReader,
            measurementConsumerId,
            reportingSetId,
            setExpression.rhs.expression,
            false
          )
        setExpressionsBinders.addAll(setExpressionsBindersAndId.setExpressionsBinders)
        rightHandSetExpressionId = setExpressionsBindersAndId.setExpressionId
        rightHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        rightHandSetExpressionId = null
        val idResult =
          reportingSetReader.readId(measurementConsumerId, setExpression.rhs.externalReportingSetId)
            ?: throw ReportingSetNotFoundException()
        rightHandReportingSetId = idResult.reportingSetId
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = null
      }
    }

    setExpressionsBinders.add {
      bind("$1", measurementConsumerId)
      bind("$2", reportingSetId)
      bind("$3", setExpressionId)
      bind("$4", root)
      bind("$5", setExpression.operationValue)
      bind("$6", leftHandSetExpressionId)
      bind("$7", leftHandReportingSetId)
      bind("$8", rightHandSetExpressionId)
      bind("$9", rightHandReportingSetId)
    }

    if (root) {
      val setExpressionsStatement =
        boundStatement(
          """
            INSERT INTO SetExpressions (MeasurementConsumerId, ReportingSetId, SetExpressionId, Root, Operation, LeftHandSetExpressionId, LeftHandReportingSetId, RightHandSetExpressionId, RightHandReportingSetId)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """
        ) {
          setExpressionsBinders.forEach { addBinding(it) }
        }
      transactionContext.executeStatement(setExpressionsStatement)
    }

    return SetExpressionsBindersAndId(setExpressionsBinders, setExpressionId)
  }

  private suspend fun TransactionScope.insertWeightedSubsetUnions(
    measurementConsumerId: Long,
    reportingSetId: Long,
    weightedSubsetUnions: List<WeightedSubsetUnion>
  ) {
    val weightedSubsetUnionsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasesBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val weightedSubsetUnionPrimitiveReportingSetBasesBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val primitiveReportingSetBasisFiltersBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    weightedSubsetUnions.forEach { weightedSubsetUnion ->
      val weightedSubsetUnionId = idGenerator.generateInternalId().value
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
            it
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

  private suspend fun TransactionScope.createPrimitiveReportingSetBasisBindings(
    measurementConsumerId: Long,
    reportingSetId: Long,
    weightedSubsetUnionId: Long,
    primitiveReportingSetBasis: PrimitiveReportingSetBasis
  ): PrimitiveReportingSetBasesBinders {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId().value

    val primitiveReportingSetIdResult =
      ReportingSetReader(transactionContext)
        .readId(measurementConsumerId, primitiveReportingSetBasis.externalReportingSetId)
        ?: throw ReportingSetNotFoundException()

    val primitiveReportingSetBasesBinder: BoundStatement.Binder.() -> Unit = {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetIdResult.reportingSetId)
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
    measurementConsumerId: Long,
    primitiveReportingSetBasisId: Long,
    filter: String
  ): BoundStatement.Binder.() -> Unit {
    val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId().value

    return {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetBasisFilterId)
      bind("$4", filter)
    }
  }
}
