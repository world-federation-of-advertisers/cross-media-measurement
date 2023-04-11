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
import org.wfanet.measurement.common.db.r2dbc.newBoundStatementBuilder
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
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
import org.wfanet.measurement.reporting.service.internal.ReportingSetInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Reporting Set into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetInvalidException] ReportingSet is invalid
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateReportingSet(private val reportingSet: ReportingSet) : PostgresWriter<ReportingSet>() {
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
        val reportingSetReader = ReportingSetReader(transactionContext)
        val insertSetExpressionBuilder =
          newBoundStatementBuilder(
            """
          INSERT INTO SetExpressions (MeasurementConsumerId, ReportingSetId, SetExpressionId, Root, Operation, LeftHandSetExpressionId, LeftHandReportingSetId, RightHandSetExpressionId, RightHandReportingSetId)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          """
          )
        insertSetExpressions(
          insertSetExpressionBuilder,
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
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        throw ReportingSetInvalidException()
      }
    }

    return reportingSet.copy { this.externalReportingSetId = externalReportingSetId.value }
  }

  private suspend fun TransactionScope.insertReportingSetEventGroups(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    eventGroups: List<ReportingSet.Primitive.EventGroupKey>
  ) {
    val insertEventGroupBuilder =
      newBoundStatementBuilder(
        """
      INSERT INTO EventGroups (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
      VALUES ($1, $2, $3, $4)
      """
      )
    val insertReportingSetEventGroupBuilder =
      newBoundStatementBuilder(
        """
      INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
      VALUES ($1, $2, $3)
      """
      )

    eventGroups.forEach {
      val eventGroupResult: EventGroupReader.Result? =
        EventGroupReader(transactionContext)
          .getByCmmsIds(
            cmmsDataProviderId = it.cmmsDataProviderId,
            cmmsEventGroupId = it.cmmsEventGroupId
          )

      val eventGroupId: InternalId
      if (eventGroupResult == null) {
        eventGroupId = idGenerator.generateInternalId()
        insertEventGroupBuilder.addBinding {
          bind("$1", measurementConsumerId)
          bind("$2", eventGroupId)
          bind("$3", it.cmmsDataProviderId)
          bind("$4", it.cmmsEventGroupId)
        }
      } else {
        eventGroupId = eventGroupResult.eventGroupId
      }

      insertReportingSetEventGroupBuilder.addBinding {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", eventGroupId)
      }
    }

    transactionContext.run {
      executeStatement(insertEventGroupBuilder.build())
      executeStatement(insertReportingSetEventGroupBuilder.build())
    }
  }

  private suspend fun TransactionScope.insertSetExpressions(
    insertSetExpressionBuilder: BoundStatement.Builder,
    reportingSetReader: ReportingSetReader,
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    setExpression: SetExpression,
    root: Boolean
  ): InternalId {
    val setExpressionId = idGenerator.generateInternalId()

    val leftHandSetExpressionId: InternalId?
    val leftHandReportingSetId: InternalId?
    val rightHandSetExpressionId: InternalId?
    val rightHandReportingSetId: InternalId?

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (setExpression.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        leftHandSetExpressionId =
          insertSetExpressions(
            insertSetExpressionBuilder,
            reportingSetReader,
            measurementConsumerId,
            reportingSetId,
            setExpression.lhs.expression,
            false
          )
        leftHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        leftHandSetExpressionId = null
        val idResult =
          reportingSetReader.readPrimaryKey(
            measurementConsumerId,
            ExternalId(setExpression.lhs.externalReportingSetId)
          )
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
        rightHandSetExpressionId =
          insertSetExpressions(
            insertSetExpressionBuilder,
            reportingSetReader,
            measurementConsumerId,
            reportingSetId,
            setExpression.rhs.expression,
            false
          )
        rightHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        rightHandSetExpressionId = null
        val idResult =
          reportingSetReader.readPrimaryKey(
            measurementConsumerId,
            ExternalId(setExpression.rhs.externalReportingSetId)
          )
            ?: throw ReportingSetNotFoundException()
        rightHandReportingSetId = idResult.reportingSetId
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = null
      }
    }

    insertSetExpressionBuilder.addBinding {
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
      transactionContext.executeStatement(insertSetExpressionBuilder.build())
    }

    return setExpressionId
  }

  private suspend fun TransactionScope.insertWeightedSubsetUnions(
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnions: List<WeightedSubsetUnion>
  ) {
    val insertWeightedSubsetUnionBuilder =
      newBoundStatementBuilder(
        """
        INSERT INTO WeightedSubsetUnions (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, Weight)
        VALUES ($1, $2, $3, $4)
        """
      )
    val insertPrimitiveReportingSetBasisBuilder =
      newBoundStatementBuilder(
        """
        INSERT INTO PrimitiveReportingSetBases (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetId)
        VALUES ($1, $2, $3)
        """
      )
    val insertWeightedSubsetUnionPrimitiveReportingSetBasisBuilder =
      newBoundStatementBuilder(
        """
        INSERT INTO WeightedSubsetUnionPrimitiveReportingSetBases (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
        VALUES ($1, $2, $3, $4)
        """
      )
    val insertPrimitiveReportingSetBasisFilterBuilder =
      newBoundStatementBuilder(
        """
        INSERT INTO PrimitiveReportingSetBasisFilters (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetBasisFilterId, Filter)
        VALUES ($1, $2, $3, $4)
        """
      )

    weightedSubsetUnions.forEach { weightedSubsetUnion ->
      val weightedSubsetUnionId = idGenerator.generateInternalId()
      insertWeightedSubsetUnionBuilder.addBinding {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", weightedSubsetUnionId)
        bind("$4", weightedSubsetUnion.weight)
      }

      weightedSubsetUnion.primitiveReportingSetBasesList.forEach {
        addPrimitiveReportingSetBasisBindings(
          insertPrimitiveReportingSetBasisBuilder,
          insertWeightedSubsetUnionPrimitiveReportingSetBasisBuilder,
          insertPrimitiveReportingSetBasisFilterBuilder,
          measurementConsumerId,
          reportingSetId,
          weightedSubsetUnionId,
          it
        )
      }
    }

    transactionContext.run {
      executeStatement(insertWeightedSubsetUnionBuilder.build())
      executeStatement(insertPrimitiveReportingSetBasisBuilder.build())
      executeStatement(insertWeightedSubsetUnionPrimitiveReportingSetBasisBuilder.build())
      executeStatement(insertPrimitiveReportingSetBasisFilterBuilder.build())
    }
  }

  private suspend fun TransactionScope.addPrimitiveReportingSetBasisBindings(
    insertPrimitiveReportingSetBasisBuilder: BoundStatement.Builder,
    insertWeightedSubsetUnionPrimitiveReportingSetBasisBuilder: BoundStatement.Builder,
    insertPrimitiveReportingSetBasisFilterBuilder: BoundStatement.Builder,
    measurementConsumerId: InternalId,
    reportingSetId: InternalId,
    weightedSubsetUnionId: InternalId,
    primitiveReportingSetBasis: PrimitiveReportingSetBasis
  ) {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId()

    val primitiveReportingSetIdResult =
      ReportingSetReader(transactionContext)
        .readPrimaryKey(
          measurementConsumerId,
          ExternalId(primitiveReportingSetBasis.externalReportingSetId)
        )
        ?: throw ReportingSetNotFoundException()

    insertPrimitiveReportingSetBasisBuilder.addBinding {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetIdResult.reportingSetId)
    }

    insertWeightedSubsetUnionPrimitiveReportingSetBasisBuilder.addBinding {
      bind("$1", measurementConsumerId)
      bind("$2", reportingSetId)
      bind("$3", weightedSubsetUnionId)
      bind("$4", primitiveReportingSetBasisId)
    }

    primitiveReportingSetBasis.filtersList.forEach {
      addPrimitiveReportingSetBasisFilterBinding(
        insertPrimitiveReportingSetBasisFilterBuilder,
        measurementConsumerId,
        primitiveReportingSetBasisId,
        it
      )
    }
  }

  private fun TransactionScope.addPrimitiveReportingSetBasisFilterBinding(
    insertPrimitiveReportingSetBasisFilterBuilder: BoundStatement.Builder,
    measurementConsumerId: InternalId,
    primitiveReportingSetBasisId: InternalId,
    filter: String
  ) {
    val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId()

    insertPrimitiveReportingSetBasisFilterBuilder.addBinding {
      bind("$1", measurementConsumerId)
      bind("$2", primitiveReportingSetBasisId)
      bind("$3", primitiveReportingSetBasisFilterId)
      bind("$4", filter)
    }
  }
}
