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

package org.wfanet.measurement.reporting.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey as CmmsMeasurementConsumerKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest as InternalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.ArgumentChangedInRequestForNextPageException
import org.wfanet.measurement.reporting.service.api.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.api.ReportingSetAlreadyExistsException
import org.wfanet.measurement.reporting.service.api.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException
import org.wfanet.measurement.reporting.v2alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsResponse

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportingSetsService(
  private val internalReportingSetsStub: ReportingSetsCoroutineStub,
  private val authorization: Authorization,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportingSetsCoroutineImplBase(coroutineContext) {
  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    val parsedRequest: ParsedCreateReportingSetRequest =
      try {
        validate(request)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: RequiredFieldNotSetException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: CampaignGroupInvalidException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    val requiredPermissions: Set<String> =
      when (request.reportingSet.valueCase) {
        ReportingSet.ValueCase.PRIMITIVE -> {
          setOf(Permission.CREATE_PRIMITIVE)
        }
        ReportingSet.ValueCase.COMPOSITE -> {
          setOf(Permission.CREATE_COMPOSITE)
        }
        ReportingSet.ValueCase.VALUE_NOT_SET ->
          error("reporting_set not specified") // Already validated.
      }
    authorization.check(request.parent, requiredPermissions)

    val referencedReportingSets =
      try {
        ReportingSets.getReferencedReportingSets(
          internalReportingSetsStub,
          parsedRequest.referencedReportingSetKeys,
        )
      } catch (e: ReportingSetNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      } catch (e: InternalReportingSetsException) {
        throw Status.INTERNAL.withDescription(e.message).withCause(e).asRuntimeException()
      }

    val internalRequest: InternalCreateReportingSetRequest =
      ReportingSets.buildInternalCreateReportingSetRequest(
        request,
        parsedRequest.parentKey,
        parsedRequest.campaignGroupKey,
        referencedReportingSets,
      )

    return try {
      internalReportingSetsStub.createReportingSet(internalRequest).toReportingSet()
    } catch (e: StatusException) {
      throw when (ReportingInternalException.getErrorCode(e)) {
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          throw MeasurementConsumerNotFoundException(request.parent, e)
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        ErrorCode.REPORTING_SET_ALREADY_EXISTS ->
          throw ReportingSetAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        ErrorCode.REPORTING_SET_NOT_FOUND ->
          throw ReportingSetNotFoundException.fromLegacyInternal(e)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        ErrorCode.CAMPAIGN_GROUP_INVALID ->
          CampaignGroupInvalidException(request.reportingSet.campaignGroup, e)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.MEASUREMENT_ALREADY_EXISTS,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_CALCULATION_TIME_INTERVAL_NOT_FOUND,
        ErrorCode.REPORT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_INVALID,
        ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS,
        ErrorCode.METRIC_ALREADY_EXISTS,
        ErrorCode.REPORT_ALREADY_EXISTS,
        ErrorCode.REPORT_SCHEDULE_ALREADY_EXISTS,
        ErrorCode.REPORT_SCHEDULE_NOT_FOUND,
        ErrorCode.REPORT_SCHEDULE_STATE_INVALID,
        ErrorCode.REPORT_SCHEDULE_ITERATION_NOT_FOUND,
        ErrorCode.REPORT_SCHEDULE_ITERATION_STATE_INVALID,
        ErrorCode.METRIC_CALCULATION_SPEC_NOT_FOUND,
        ErrorCode.METRIC_CALCULATION_SPEC_ALREADY_EXISTS,
        ErrorCode.UNRECOGNIZED,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  private data class ParsedCreateReportingSetRequest(
    val request: CreateReportingSetRequest,
    val parentKey: CmmsMeasurementConsumerKey,
    val campaignGroupKey: ReportingSetKey?,
    val referencedReportingSetKeys: Set<ReportingSetKey>,
  )

  /**
   * Validates [request].
   *
   * @throws InvalidFieldValueException
   * @throws RequiredFieldNotSetException
   * @throws CampaignGroupInvalidException
   */
  private fun validate(request: CreateReportingSetRequest): ParsedCreateReportingSetRequest {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
    }
    val parentKey =
      CmmsMeasurementConsumerKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
    if (!request.hasReportingSet()) {
      throw RequiredFieldNotSetException("reporting_set")
    }
    if (request.reportingSetId.isEmpty()) {
      throw RequiredFieldNotSetException("reporting_set_id")
    }
    if (!request.reportingSetId.matches(RESOURCE_ID_REGEX)) {
      throw InvalidFieldValueException("reporting_set_id")
    }

    val referencedReportingSetKeys: Set<ReportingSetKey> =
      when (request.reportingSet.valueCase) {
        ReportingSet.ValueCase.PRIMITIVE -> {
          if (request.reportingSet.primitive.cmmsEventGroupsList.isEmpty()) {
            throw RequiredFieldNotSetException("reporting_set.primitive.cmms_event_groups")
          }
          request.reportingSet.primitive.cmmsEventGroupsList.forEachIndexed {
            index,
            cmmsEventGroupName ->
            if (CmmsEventGroupKey.fromName(cmmsEventGroupName) == null) {
              throw InvalidFieldValueException("reporting_set.primitive.cmms_event_groups[$index]")
            }
          }
          emptySet()
        }
        ReportingSet.ValueCase.COMPOSITE -> {
          if (!request.reportingSet.composite.hasExpression()) {
            throw RequiredFieldNotSetException("reporting_set.composite.expression")
          }
          validateSetExpression(
            parentKey,
            request.reportingSet.composite.expression,
            "reporting_set.composite.expression",
          )
        }
        ReportingSet.ValueCase.VALUE_NOT_SET ->
          throw RequiredFieldNotSetException("reporting_set.value")
      }

    val campaignGroupKey: ReportingSetKey? =
      if (request.reportingSet.campaignGroup.isEmpty()) {
        null
      } else {
        ReportingSetKey.fromName(request.reportingSet.campaignGroup)
          ?: throw InvalidFieldValueException("reporting_set.campaign_group")
      }
    if (campaignGroupKey != null) {
      if (campaignGroupKey.parentKey != parentKey) {
        throw InvalidFieldValueException("reporting_set.campaign_group") { fieldPath ->
          "$fieldPath belongs to a different MeasurementConsumer"
        }
      }
      if (
        campaignGroupKey.reportingSetId == request.reportingSetId &&
          !request.reportingSet.hasPrimitive()
      ) {
        throw CampaignGroupInvalidException(request.reportingSet.campaignGroup)
      }
    }

    return ParsedCreateReportingSetRequest(
      request,
      parentKey,
      campaignGroupKey,
      referencedReportingSetKeys,
    )
  }

  /**
   * Validates a request [setExpression].
   *
   * @return the set of keys of the referenced ReportingSets
   * @throws RequiredFieldNotSetException
   * @throws InvalidFieldValueException
   */
  private fun validateSetExpression(
    parentKey: CmmsMeasurementConsumerKey,
    setExpression: ReportingSet.SetExpression,
    fieldPath: String,
  ): Set<ReportingSetKey> {
    when (setExpression.operation) {
      ReportingSet.SetExpression.Operation.UNION,
      ReportingSet.SetExpression.Operation.DIFFERENCE,
      ReportingSet.SetExpression.Operation.INTERSECTION -> {}
      ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED ->
        throw RequiredFieldNotSetException("$fieldPath.operation")
      ReportingSet.SetExpression.Operation.UNRECOGNIZED ->
        throw InvalidFieldValueException("$fieldPath.operation")
    }

    if (!setExpression.hasLhs()) {
      throw RequiredFieldNotSetException("$fieldPath.lhs")
    }

    return buildSet {
      addAll(validateOperand(parentKey, setExpression.lhs, "$fieldPath.lhs"))
      if (setExpression.hasRhs()) {
        addAll(validateOperand(parentKey, setExpression.rhs, "$fieldPath.rhs"))
      }
    }
  }

  /**
   * Validates a request [operand].
   *
   * @return the set of keys of the referenced ReportingSets
   * @throws InvalidFieldValueException
   * @throws RequiredFieldNotSetException
   */
  private fun validateOperand(
    parentKey: CmmsMeasurementConsumerKey,
    operand: ReportingSet.SetExpression.Operand,
    fieldPath: String,
  ): Set<ReportingSetKey> {
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET -> {
        val reportingSetKey =
          ReportingSetKey.fromName(operand.reportingSet)
            ?: throw InvalidFieldValueException("$fieldPath.reporting_set")
        if (reportingSetKey.parentKey != parentKey) {
          throw InvalidFieldValueException("$fieldPath.reporting_set") { invalidFieldPath ->
            "$invalidFieldPath does not belong to the same MeasurementConsumer"
          }
        }
        setOf(reportingSetKey)
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        validateSetExpression(parentKey, operand.expression, "$fieldPath.expression")
      }
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET ->
        throw RequiredFieldNotSetException("$fieldPath.operand")
    }
  }

  override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val reportingSetKey =
      ReportingSetKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    authorization.check(listOf(request.name, reportingSetKey.parentKey.toName()), Permission.GET)

    val internalResponse =
      try {
        internalReportingSetsStub.batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = reportingSetKey.parentKey.measurementConsumerId
            externalReportingSetIds += reportingSetKey.reportingSetId
          }
        )
      } catch (e: StatusException) {
        throw when (ReportingInternalException.getErrorCode(e)) {
          ErrorCode.REPORTING_SET_NOT_FOUND ->
            throw ReportingSetNotFoundException.fromLegacyInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          ErrorCode.UNKNOWN_ERROR,
          ErrorCode.REPORTING_SET_ALREADY_EXISTS,
          ErrorCode.MEASUREMENT_ALREADY_EXISTS,
          ErrorCode.MEASUREMENT_NOT_FOUND,
          ErrorCode.MEASUREMENT_CALCULATION_TIME_INTERVAL_NOT_FOUND,
          ErrorCode.REPORT_NOT_FOUND,
          ErrorCode.MEASUREMENT_STATE_INVALID,
          ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
          ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS,
          ErrorCode.METRIC_ALREADY_EXISTS,
          ErrorCode.REPORT_ALREADY_EXISTS,
          ErrorCode.REPORT_SCHEDULE_ALREADY_EXISTS,
          ErrorCode.REPORT_SCHEDULE_NOT_FOUND,
          ErrorCode.REPORT_SCHEDULE_STATE_INVALID,
          ErrorCode.REPORT_SCHEDULE_ITERATION_NOT_FOUND,
          ErrorCode.REPORT_SCHEDULE_ITERATION_STATE_INVALID,
          ErrorCode.METRIC_CALCULATION_SPEC_NOT_FOUND,
          ErrorCode.METRIC_CALCULATION_SPEC_ALREADY_EXISTS,
          ErrorCode.CAMPAIGN_GROUP_INVALID,
          ErrorCode.UNRECOGNIZED,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.reportingSetsList.single().toReportingSet()
  }

  override suspend fun listReportingSets(
    request: ListReportingSetsRequest
  ): ListReportingSetsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val parentKey =
      CmmsMeasurementConsumerKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    val listReportingSetsPageToken: ListReportingSetsPageToken =
      try {
        request.toListReportingSetsPageToken(parentKey)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: ArgumentChangedInRequestForNextPageException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    authorization.check(request.parent, Permission.LIST)

    val results: List<InternalReportingSet> =
      try {
        internalReportingSetsStub
          .streamReportingSets(listReportingSetsPageToken.toStreamReportingSetsRequest())
          .toList()
      } catch (e: StatusException) {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListReportingSetsResponse.getDefaultInstance()
    }

    return listReportingSetsResponse {
      reportingSets +=
        results
          .subList(0, min(results.size, listReportingSetsPageToken.pageSize))
          .map(InternalReportingSet::toReportingSet)

      if (results.size > listReportingSetsPageToken.pageSize) {
        val pageToken =
          listReportingSetsPageToken.copy {
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = results[results.lastIndex - 1].cmmsMeasurementConsumerId
              externalReportingSetId = results[results.lastIndex - 1].externalReportingSetId
            }
          }
        nextPageToken = pageToken.toByteString().base64UrlEncode()
      }
    }
  }

  object Permission {
    private const val TYPE = "reporting.reportingSets"
    const val CREATE_PRIMITIVE = "$TYPE.createPrimitive"
    const val CREATE_COMPOSITE = "$TYPE.createComposite"
    const val GET = "$TYPE.get"
    const val LIST = "$TYPE.list"
  }

  companion object {
    private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX
  }
}

/**
 * Converts a public [ListReportingSetsRequest] to a [ListReportingSetsPageToken].
 *
 * @throws InvalidFieldValueException
 * @throws ArgumentChangedInRequestForNextPageException
 */
private fun ListReportingSetsRequest.toListReportingSetsPageToken(
  parentKey: CmmsMeasurementConsumerKey
): ListReportingSetsPageToken {
  if (pageSize < 0) {
    throw InvalidFieldValueException("page_size") { fieldPath ->
      "$fieldPath cannot be less than 0"
    }
  }

  val source = this
  return if (source.pageToken.isNotEmpty()) {
    ListReportingSetsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      if (this.cmmsMeasurementConsumerId != parentKey.measurementConsumerId) {
        throw ArgumentChangedInRequestForNextPageException("parent")
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportingSetsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.cmmsMeasurementConsumerId = parentKey.measurementConsumerId
    }
  }
}
