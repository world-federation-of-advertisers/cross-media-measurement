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

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
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
import org.wfanet.measurement.reporting.service.internal.ReportingSetAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Reporting Set into the database.
 */
class CreateReportingSet(private val reportingSet: ReportingSet) : PostgresWriter<ReportingSet>() {
  override suspend fun TransactionScope.runTransaction(): ReportingSet {
    val measurementConsumerId = (MeasurementConsumerReader(transactionContext).getByCmmsId(reportingSet.cmmsMeasurementConsumerId)
      ?: throw MeasurementConsumerNotFoundException()).measurementConsumerId

    val internalReportingSetId = idGenerator.generateInternalId().value
    val externalReportingSetId = idGenerator.generateExternalId().value

    try {
      val statement =
        boundStatement(
          """
        INSERT INTO ReportingSets (MeasurementConsumerId, ReportingSetId, ExternalReportingSetId, DisplayName, Filter)
          VALUES ($1, $2, $3, $4, $5)
        """
        ) {
          bind("$1", measurementConsumerId)
          bind("$2", internalReportingSetId)
          bind("$3", externalReportingSetId)
          bind("$4", reportingSet.displayName)
          bind("$5", reportingSet.filter)
        }

      transactionContext.executeStatement(statement)

      if (reportingSet.valueCase.equals(ReportingSet.ValueCase.PRIMITIVE)) {
        insertReportingSetEventGroups(measurementConsumerId, internalReportingSetId, reportingSet.primitive.eventGroupKeysList)
      } else if (reportingSet.valueCase.equals(ReportingSet.ValueCase.COMPOSITE)) {
        val reportingSetReader = ReportingSetReader(transactionContext)
        insertSetExpression(reportingSetReader, measurementConsumerId, internalReportingSetId, reportingSet.composite, true)
        reportingSet.weightedSubsetUnionsList.forEach {
          insertWeightedSubsetUnion(measurementConsumerId, internalReportingSetId, it)
        }
      }
    } catch (e: R2dbcDataIntegrityViolationException) {
      throw ReportingSetAlreadyExistsException()
    }

    return reportingSet.copy {
      this.externalReportingSetId = externalReportingSetId
    }
  }

  private suspend fun TransactionScope.insertReportingSetEventGroups(measurementConsumerId: Long,
                                                                     reportingSetId: Long,
                                                                     eventGroups: List<ReportingSet.Primitive.EventGroupKey>) {
    transactionContext.run {
      eventGroups.forEach {
        val eventGroupResult: EventGroupReader.Result? = EventGroupReader(transactionContext).getByCmmsIds(
          cmmsDataProviderId = it.cmmsDataProviderId,
          cmmsEventGroupId = it.cmmsEventGroupId
        )

        val eventGroupId: Long
        if (eventGroupResult == null) {
          eventGroupId = idGenerator.generateInternalId().value
          val statement =
            boundStatement(
              """
              INSERT INTO EventGroups (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
              VALUES ($1, $2, $3, $4)
              """
            ) {
              bind("$1", measurementConsumerId)
              bind("$2", eventGroupId)
              bind("$3", it.cmmsDataProviderId)
              bind("$4", it.cmmsEventGroupId)
            }
          executeStatement(statement)
        } else {
          eventGroupId = eventGroupResult.eventGroupId
        }

        val statement =
          boundStatement(
            """
            INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
            VALUES ($1, $2, $3)
            """
          ) {
            bind("$1", measurementConsumerId)
            bind("$2", reportingSetId)
            bind("$3", eventGroupId)
          }
        executeStatement(statement)
      }
    }
  }

  private suspend fun TransactionScope.insertSetExpression(reportingSetReader: ReportingSetReader,
                                                           measurementConsumerId: Long, reportingSetId: Long, setExpression: SetExpression, root: Boolean): Long {
    val setExpressionId = idGenerator.generateInternalId().value

    val leftHandSetExpressionId: Long?
    val leftHandReportingSetId: Long?
    val rightHandSetExpressionId: Long?
    val rightHandReportingSetId: Long?

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when(setExpression.lhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        leftHandSetExpressionId = insertSetExpression(reportingSetReader, measurementConsumerId, reportingSetId, setExpression.lhs.expression, false)
        leftHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        leftHandSetExpressionId = null
        val idResult = reportingSetReader.readId(measurementConsumerId, setExpression.lhs.externalReportingSetId)
          ?: throw ReportingSetNotFoundException()
        leftHandReportingSetId = idResult.reportingSetId
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        leftHandSetExpressionId = null
        leftHandReportingSetId = null
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when(setExpression.rhs.operandCase) {
      SetExpression.Operand.OperandCase.EXPRESSION -> {
        rightHandSetExpressionId = insertSetExpression(reportingSetReader, measurementConsumerId, reportingSetId, setExpression.rhs.expression, false)
        rightHandReportingSetId = null
      }
      SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        rightHandSetExpressionId = null
        val idResult = reportingSetReader.readId(measurementConsumerId, setExpression.rhs.externalReportingSetId)
          ?: throw ReportingSetNotFoundException()
        rightHandReportingSetId = idResult.reportingSetId
      }
      SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        rightHandSetExpressionId = null
        rightHandReportingSetId = null
      }
    }

    val statement =
      boundStatement(
        """
            INSERT INTO SetExpressions (MeasurementConsumerId, ReportingSetId, SetExpressionId, Root, Operation, LeftHandSetExpressionId, LeftHandReportingSetId, RightHandSetExpressionId, RightHandReportingSetId)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """
      ) {
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

    transactionContext.executeStatement(statement)

    return setExpressionId
  }

  private suspend fun TransactionScope.insertWeightedSubsetUnion(measurementConsumerId: Long, reportingSetId: Long, weightedSubsetUnion: WeightedSubsetUnion) {
    val weightedSubsetUnionId = idGenerator.generateInternalId().value

    val statement =
      boundStatement(
        """
            INSERT INTO WeightedSubsetUnions (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, Weight)
            VALUES ($1, $2, $3, $4)
            """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", weightedSubsetUnionId)
        bind("$4", weightedSubsetUnion.weight)
      }

    transactionContext.executeStatement(statement)

    weightedSubsetUnion.primitiveReportingSetBasesList.forEach {
      insertPrimitiveReportingSetBasis(measurementConsumerId, reportingSetId, weightedSubsetUnionId, it)
    }
  }

  private suspend fun TransactionScope.insertPrimitiveReportingSetBasis(measurementConsumerId: Long, reportingSetId: Long, weightedSubsetUnionId: Long, primitiveReportingSetBasis: PrimitiveReportingSetBasis) {
    val primitiveReportingSetBasisId = idGenerator.generateInternalId().value

    val primitiveReportingSetBasesStatement =
      boundStatement(
        """
            INSERT INTO PrimitiveReportingSetBases (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetId)
            VALUES ($1, $2, $3)
            """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", primitiveReportingSetBasisId)
        bind("$3", primitiveReportingSetBasis.externalReportingSetId)
      }

    transactionContext.executeStatement(primitiveReportingSetBasesStatement)

    val weightedSubsetUnionPrimitiveReportingSetBasesStatement =
      boundStatement(
        """
            INSERT INTO WeightedSubsetUnionPrimitiveReportingSetBases (MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
            VALUES ($1, $2, $3, $4)
            """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportingSetId)
        bind("$3", weightedSubsetUnionId)
        bind("$4", primitiveReportingSetBasisId)
      }

    transactionContext.executeStatement(weightedSubsetUnionPrimitiveReportingSetBasesStatement)

    primitiveReportingSetBasis.filtersList.forEach {
      insertPrimitiveReportingSetBasisFilter(measurementConsumerId, primitiveReportingSetBasisId, it)
    }
  }

  private suspend fun TransactionScope.insertPrimitiveReportingSetBasisFilter(measurementConsumerId: Long, primitiveReportingSetBasisId: Long, filter: String) {
    val primitiveReportingSetBasisFilterId = idGenerator.generateInternalId().value

    val statement =
      boundStatement(
        """
            INSERT INTO PrimitiveReportingSetBasisFilters (MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetBasisFilterId, Filter)
            VALUES ($1, $2, $3, $4)
            """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", primitiveReportingSetBasisId)
        bind("$3", primitiveReportingSetBasisFilterId)
        bind("$4", filter)
      }

    transactionContext.executeStatement(statement)
  }
}
