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

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.SetExpression
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.reporting.service.internal.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

private typealias Translate = (row: ResultRow) -> Unit

class ReportingSetReader(private val readContext: ReadContext) {
  data class ReportingSetIds(
    val measurementConsumerId: InternalId,
    val reportingSetId: InternalId,
    val externalReportingSetId: String,
  )

  data class Result(
    val measurementConsumerId: InternalId,
    val reportingSetId: InternalId,
    val reportingSet: ReportingSet,
  )

  private data class ReportingSetInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val reportingSetId: InternalId,
    val externalReportingSetId: String,
    val campaignGroupId: InternalId?,
    val externalCampaignGroupId: String?,
    val displayName: String?,
    val filter: String?,
    val setExpressionId: InternalId?,
    val cmmsEventGroupIdsSet: MutableSet<CmmsEventGroupIds>,
    // Key is setExpressionId.
    val setExpressionInfoMap: MutableMap<InternalId, SetExpressionInfo>,
    // Key is weightedSubsetUnionId.
    val weightedSubsetUnionInfoMap: MutableMap<InternalId, WeightedSubsetUnionInfo>,
    val details: ReportingSet.Details,
  )

  private data class SetExpressionInfo(
    val operation: SetExpression.Operation,
    val leftHandSetExpressionId: InternalId?,
    val leftHandExternalReportingSetId: String?,
    val rightHandSetExpressionId: InternalId?,
    val rightHandExternalReportingSetId: String?,
  )

  private data class WeightedSubsetUnionInfo(
    val weight: Int,
    val binaryRepresentation: Int,
    // Key is primitiveReportingSetBasisId.
    val primitiveReportingSetBasisInfoMap: MutableMap<InternalId, PrimitiveReportingSetBasisInfo>,
  )

  private data class PrimitiveReportingSetBasisInfo(
    val primitiveExternalReportingSetId: String,
    val filterSet: MutableSet<String>,
  )

  private data class CmmsEventGroupIds(
    val cmmsDataProviderId: String,
    val cmmsEventGroupId: String,
  )

  private val baseSqlSelect: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      ReportingSets.MeasurementConsumerId AS ReportingSetsMeasurementConsumerId,
      ReportingSets.ReportingSetId,
      ReportingSets.CampaignGroupId,
      CampaignGroupReportingSets.ExternalReportingSetId AS ExternalCampaignGroupId,
      ReportingSets.ExternalReportingSetId AS RootExternalReportingSetId,
      ReportingSets.SetExpressionId AS RootSetExpressionId,
      ReportingSets.DisplayName,
      ReportingSets.Filter AS ReportingSetFilter,
      ReportingSets.ReportingSetDetails,
      WeightedSubsetUnionId,
      WeightedSubsetUnions.Weight,
      WeightedSubsetUnions.BinaryRepresentation,
      PrimitiveReportingSetBasisId,
      PrimitiveReportingSets.ExternalReportingSetId AS PrimitiveExternalReportingSetId,
      PrimitiveReportingSetBasisFilters.Filter AS PrimitiveReportingSetBasisFilter,
      CmmsDataProviderId,
      CmmsEventGroupId,
      SetExpressions.SetExpressionId AS SetExpressionId,
      Operation,
      LeftHandSetExpressionId,
      LeftHandReportingSets.ExternalReportingSetId AS LeftHandExternalReportingSetId,
      RightHandSetExpressionId,
      RightHandReportingSets.ExternalReportingSetId AS RightHandExternalReportingSetId
    """
      .trimIndent()

  private val baseSqlJoins: String =
    """
    LEFT JOIN WeightedSubsetUnions USING(MeasurementConsumerId, ReportingSetId)
    LEFT JOIN WeightedSubsetUnionPrimitiveReportingSetBases USING(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId)
    LEFT JOIN PrimitiveReportingSetBases USING(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    LEFT JOIN PrimitiveReportingSetBasisFilters USING(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    LEFT JOIN ReportingSetEventGroups USING(MeasurementConsumerId, ReportingSetId)
    LEFT JOIN EventGroups USING(MeasurementConsumerId, EventGroupId)
    LEFT JOIN SetExpressions USING(MeasurementConsumerId, ReportingSetId)
    LEFT JOIN ReportingSets AS PrimitiveReportingSets ON PrimitiveReportingSetBases.MeasurementConsumerId = PrimitiveReportingSets.MeasurementConsumerId
      AND PrimitiveReportingSetBases.PrimitiveReportingSetId = PrimitiveReportingSets.ReportingSetId
    LEFT JOIN ReportingSets AS LeftHandReportingSets ON SetExpressions.MeasurementConsumerId = LeftHandReportingSets.MeasurementConsumerId
      AND SetExpressions.LeftHandReportingSetId = LeftHandReportingSets.ReportingSetId
    LEFT JOIN ReportingSets AS RightHandReportingSets ON SetExpressions.MeasurementConsumerId = RightHandReportingSets.MeasurementConsumerId
      AND SetExpressions.RightHandReportingSetId = RightHandReportingSets.ReportingSetId
    LEFT JOIN ReportingSets AS CampaignGroupReportingSets ON
      ReportingSets.MeasurementConsumerId = CampaignGroupReportingSets.MeasurementConsumerId
      AND ReportingSets.CampaignGroupId = CampaignGroupReportingSets.CampaignGroupId
    """
      .trimIndent()

  /**
   * Reads multiple ReportingSets using a single query.
   *
   * @throws [ReportingSetNotFoundException] if any ReportingSet not found.
   */
  fun batchGetReportingSets(request: BatchGetReportingSetsRequest): Flow<Result> {
    require(request.cmmsMeasurementConsumerId.isNotEmpty())
    val sql =
      StringBuilder(
        """
          $baseSqlSelect
          FROM MeasurementConsumers
            JOIN ReportingSets USING(MeasurementConsumerId)
          $baseSqlJoins
          WHERE CmmsMeasurementConsumerId = $1
            AND ReportingSets.ExternalReportingSetId IN
        """
          .trimIndent()
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      request.externalReportingSetIdsList.joinToString(
        separator = ",",
        prefix = "(",
        postfix = ")",
      ) {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", request.cmmsMeasurementConsumerId)
        request.externalReportingSetIdsList.forEach { bind(bindingMap.getValue(it), it) }
      }

    return flow {
      val reportingSetInfoMap = buildResultMap(statement)

      for (externalReportingSetId in request.externalReportingSetIdsList) {
        val reportingSetInfo =
          reportingSetInfoMap[externalReportingSetId]
            ?: throw ReportingSetNotFoundException(
              request.cmmsMeasurementConsumerId,
              externalReportingSetId,
            )

        val reportingSet = reportingSetInfo.buildReportingSet()

        emit(
          Result(
            measurementConsumerId = reportingSetInfo.measurementConsumerId,
            reportingSetId = reportingSetInfo.reportingSetId,
            reportingSet,
          )
        )
      }
    }
  }

  fun readReportingSets(request: StreamReportingSetsRequest): Flow<Result> {
    val whereClause = buildString {
      append(
        "WHERE MeasurementConsumerId IN (SELECT MeasurementConsumerId FROM MeasurementConsumers WHERE CmmsMeasurementConsumerId = $1)"
      )
      if (request.filter.externalCampaignGroupId.isNotEmpty()) {
        append(
          "AND CampaignGroupId IN (SELECT ReportingSetId FROM ReportingSets WHERE ExternalReportingSetId = $4)"
        )
      }
      append("AND ExternalReportingSetId > $2")
    }

    val sql =
      """
        $baseSqlSelect
        FROM (
            SELECT *
            FROM MeasurementConsumers
              JOIN ReportingSets USING (MeasurementConsumerId)
            $whereClause
            ORDER BY ExternalReportingSetId ASC
            LIMIT $3
          ) AS ReportingSets
        $baseSqlJoins
        ORDER BY RootExternalReportingSetId ASC
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalReportingSetIdAfter)
        if (request.limit > 0) {
          bind("$3", request.limit)
        } else {
          bind("$3", 50)
        }
        if (request.filter.externalCampaignGroupId.isNotEmpty()) {
          bind("$4", request.filter.externalCampaignGroupId)
        }
      }

    return flow {
      val reportingSetInfoMap = buildResultMap(statement)

      for (entry in reportingSetInfoMap) {
        val reportingSetInfo = entry.value
        val reportingSet = reportingSetInfo.buildReportingSet()

        emit(
          Result(
            measurementConsumerId = reportingSetInfo.measurementConsumerId,
            reportingSetId = reportingSetInfo.reportingSetId,
            reportingSet,
          )
        )
      }
    }
  }

  private fun ReportingSetInfo.buildReportingSet(): ReportingSet {
    val reportingSetInfo = this
    return reportingSet {
      cmmsMeasurementConsumerId = reportingSetInfo.cmmsMeasurementConsumerId
      externalReportingSetId = reportingSetInfo.externalReportingSetId
      if (reportingSetInfo.externalCampaignGroupId != null) {
        externalCampaignGroupId = reportingSetInfo.externalCampaignGroupId
      }
      if (reportingSetInfo.displayName != null) {
        displayName = reportingSetInfo.displayName
      }
      if (!reportingSetInfo.filter.isNullOrBlank()) {
        filter = reportingSetInfo.filter
      }
      if (reportingSetInfo.details != ReportingSet.Details.getDefaultInstance()) {
        details = reportingSetInfo.details
      }

      if (reportingSetInfo.cmmsEventGroupIdsSet.size > 0) {
        primitive =
          ReportingSetKt.primitive {
            reportingSetInfo.cmmsEventGroupIdsSet.forEach {
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = it.cmmsDataProviderId
                  cmmsEventGroupId = it.cmmsEventGroupId
                }
            }
          }

        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            weight = 1
            binaryRepresentation = 1
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                this.externalReportingSetId = reportingSetInfo.externalReportingSetId
                if (!reportingSetInfo.filter.isNullOrBlank()) {
                  filters += reportingSetInfo.filter
                }
              }
          }
      }

      if (reportingSetInfo.setExpressionInfoMap.isNotEmpty()) {
        reportingSetInfo.setExpressionInfoMap[reportingSetInfo.setExpressionId]?.let {
          composite = buildSetExpression(it, reportingSetInfo.setExpressionInfoMap)
        }

        reportingSetInfo.weightedSubsetUnionInfoMap.values.forEach {
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = it.weight
              binaryRepresentation = it.binaryRepresentation
              it.primitiveReportingSetBasisInfoMap.values.forEach {
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    externalReportingSetId = it.primitiveExternalReportingSetId
                    filters += it.filterSet
                  }
              }
            }
        }
      }
    }
  }

  /** Returns a map that maintains the order of the query result. */
  private suspend fun buildResultMap(statement: BoundStatement): Map<String, ReportingSetInfo> {
    // Key is externalReportingSetId.
    val reportingSetInfoMap: MutableMap<String, ReportingSetInfo> = linkedMapOf()

    val translate: Translate = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["ReportingSetsMeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val reportingSetId: InternalId = row["ReportingSetId"]
      val externalReportingSetId: String = row["RootExternalReportingSetId"]
      val campaignGroupId: InternalId? = row["CampaignGroupId"]
      val externalCampaignGroupId: String? = row["ExternalCampaignGroupId"]
      val rootSetExpressionId: InternalId? = row["RootSetExpressionId"]
      val displayName: String? = row["DisplayName"]
      val reportingSetFilter: String? = row["ReportingSetFilter"]
      val weightedSubsetUnionId: InternalId? = row["WeightedSubsetUnionId"]
      val weight: Int? = row["Weight"]
      val binaryRepresentation: Int? = row["BinaryRepresentation"]
      val primitiveReportingSetBasisId: InternalId? = row["PrimitiveReportingSetBasisId"]
      val primitiveExternalReportingSetId: String? = row["PrimitiveExternalReportingSetId"]
      val primitiveReportingSetBasisFilter: String? = row["PrimitiveReportingSetBasisFilter"]
      val cmmsDataProviderId: String? = row["CmmsDataProviderId"]
      val cmmsEventGroupId: String? = row["CmmsEventGroupId"]
      val setExpressionId: InternalId? = row["SetExpressionId"]
      val operation: Int? = row["Operation"]
      val leftHandSetExpressionId: InternalId? = row["LeftHandSetExpressionId"]
      val leftHandExternalReportingSetId: String? = row["LeftHandExternalReportingSetId"]
      val rightHandSetExpressionId: InternalId? = row["RightHandSetExpressionId"]
      val rightHandExternalReportingSetId: String? = row["RightHandExternalReportingSetId"]
      val reportingSetDetails: ReportingSet.Details =
        row.getProtoMessage("ReportingSetDetails", ReportingSet.Details.parser())

      val reportingSetInfo =
        reportingSetInfoMap.computeIfAbsent(externalReportingSetId) {
          ReportingSetInfo(
            measurementConsumerId = measurementConsumerId,
            cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
            reportingSetId = reportingSetId,
            externalReportingSetId = externalReportingSetId,
            campaignGroupId = campaignGroupId,
            externalCampaignGroupId = externalCampaignGroupId,
            displayName = displayName,
            filter = reportingSetFilter,
            setExpressionId = rootSetExpressionId,
            cmmsEventGroupIdsSet = mutableSetOf(),
            setExpressionInfoMap = mutableMapOf(),
            weightedSubsetUnionInfoMap = mutableMapOf(),
            reportingSetDetails,
          )
        }

      if (setExpressionId != null && operation != null) {
        reportingSetInfo.setExpressionInfoMap.computeIfAbsent(setExpressionId) {
          SetExpressionInfo(
            operation = SetExpression.Operation.forNumber(operation),
            leftHandSetExpressionId = leftHandSetExpressionId,
            leftHandExternalReportingSetId = leftHandExternalReportingSetId,
            rightHandSetExpressionId = rightHandSetExpressionId,
            rightHandExternalReportingSetId = rightHandExternalReportingSetId,
          )
        }
      }

      if (cmmsDataProviderId != null && cmmsEventGroupId != null) {
        reportingSetInfo.cmmsEventGroupIdsSet.add(
          CmmsEventGroupIds(
            cmmsDataProviderId = cmmsDataProviderId,
            cmmsEventGroupId = cmmsEventGroupId,
          )
        )
      }

      if (
        weightedSubsetUnionId != null &&
          weight != null &&
          binaryRepresentation != null &&
          primitiveReportingSetBasisId != null &&
          primitiveExternalReportingSetId != null
      ) {
        val weightedSubsetUnionInfo =
          reportingSetInfo.weightedSubsetUnionInfoMap.computeIfAbsent(weightedSubsetUnionId) {
            WeightedSubsetUnionInfo(
              weight = weight,
              binaryRepresentation = binaryRepresentation,
              primitiveReportingSetBasisInfoMap = mutableMapOf(),
            )
          }

        val primitiveReportingSetBasisInfo =
          weightedSubsetUnionInfo.primitiveReportingSetBasisInfoMap.computeIfAbsent(
            primitiveReportingSetBasisId
          ) {
            PrimitiveReportingSetBasisInfo(
              primitiveExternalReportingSetId = primitiveExternalReportingSetId,
              filterSet = mutableSetOf(),
            )
          }

        if (primitiveReportingSetBasisFilter != null) {
          primitiveReportingSetBasisInfo.filterSet.add(primitiveReportingSetBasisFilter)
        }
      }
    }

    readContext.executeQuery(statement).consume(translate).collect {}

    return reportingSetInfoMap
  }

  private fun buildSetExpression(
    setExpressionInfo: SetExpressionInfo,
    setExpressionInfoMap: Map<InternalId, SetExpressionInfo>,
  ): SetExpression {
    return ReportingSetKt.setExpression {
      operation = setExpressionInfo.operation

      if (setExpressionInfo.leftHandExternalReportingSetId != null) {
        lhs =
          ReportingSetKt.SetExpressionKt.operand {
            externalReportingSetId = setExpressionInfo.leftHandExternalReportingSetId
          }
      } else if (setExpressionInfo.leftHandSetExpressionId != null) {
        setExpressionInfoMap[setExpressionInfo.leftHandSetExpressionId]?.let {
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression = buildSetExpression(it, setExpressionInfoMap)
            }
        }
      }

      if (setExpressionInfo.rightHandExternalReportingSetId != null) {
        rhs =
          ReportingSetKt.SetExpressionKt.operand {
            externalReportingSetId = setExpressionInfo.rightHandExternalReportingSetId
          }
      } else if (setExpressionInfo.rightHandSetExpressionId != null) {
        setExpressionInfoMap[setExpressionInfo.rightHandSetExpressionId]?.let {
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression = buildSetExpression(it, setExpressionInfoMap)
            }
        }
      }
    }
  }

  suspend fun readIds(
    measurementConsumerId: InternalId,
    externalReportingSetIds: Collection<String>,
  ): Flow<ReportingSetIds> {
    if (externalReportingSetIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        """
        SELECT
          MeasurementConsumerId,
          ReportingSetId,
          ExternalReportingSetId
        FROM ReportingSets
        WHERE MeasurementConsumerId = $1
          AND ExternalReportingSetId IN
        """
      )

    var i = 2
    val bindingMap = mutableMapOf<String, String>()
    val inList =
      externalReportingSetIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        externalReportingSetIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return readContext.executeQuery(statement).consume { row: ResultRow ->
      ReportingSetIds(
        row["MeasurementConsumerId"],
        row["ReportingSetId"],
        row["ExternalReportingSetId"],
      )
    }
  }

  /**
   * Reads a ReportingSet that is a Campaign Group.
   *
   * @return the IDs for the ReportingSet, or `null` if not found
   * @throws CampaignGroupInvalidException if the ReportingSet is not a Campaign Group
   */
  suspend fun readCampaignGroup(
    measurementConsumerId: InternalId,
    externalReportingSetId: String,
  ): ReportingSetIds? {
    return readContext
      .executeQuery(
        boundStatement(
          """
          SELECT
            ReportingSetId,
            ExternalReportingSetId,
            CampaignGroupId,
            CmmsMeasurementConsumerId
          FROM
            ReportingSets
            JOIN MeasurementConsumers USING (MeasurementConsumerId)
          WHERE
            MeasurementConsumerId = $1
            AND ExternalReportingSetId = $2
          """
            .trimIndent()
        ) {
          bind("$1", measurementConsumerId)
          bind("$2", externalReportingSetId)
        }
      )
      .consume { row ->
        val reportingSetId: InternalId = row["ReportingSetId"]
        val campaignGroupId: InternalId? = row["CampaignGroupId"]
        if (campaignGroupId != reportingSetId) {
          throw CampaignGroupInvalidException(
            row["CmmsMeasurementConsumerId"],
            externalReportingSetId,
          )
        }

        ReportingSetIds(measurementConsumerId, reportingSetId, externalReportingSetId)
      }
      .singleOrNullIfEmpty()
  }
}
