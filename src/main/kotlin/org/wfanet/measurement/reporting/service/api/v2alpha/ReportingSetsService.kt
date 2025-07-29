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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest as InternalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest as internalCreateReportingSetRequest
import org.wfanet.measurement.reporting.service.api.CampaignGroupInvalidException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.api.ServiceException
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
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    grpcRequire(request.hasReportingSet()) { "ReportingSet is not specified." }
    grpcRequire(request.reportingSetId.matches(RESOURCE_ID_REGEX)) {
      "Reporting set ID is invalid."
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    val requiredPermissions: Set<String> =
      when (request.reportingSet.valueCase) {
        ReportingSet.ValueCase.PRIMITIVE -> {
          validateEventGroups(request.reportingSet.primitive)
          setOf(Permission.CREATE_PRIMITIVE)
        }
        ReportingSet.ValueCase.COMPOSITE -> {
          validateSetExpression(parentKey, request.reportingSet.composite.expression)
          setOf(Permission.CREATE_COMPOSITE)
        }
        ReportingSet.ValueCase.VALUE_NOT_SET ->
          throw Status.INVALID_ARGUMENT.withDescription("reporting_set.value not set")
            .asRuntimeException()
      }
    authorization.check(request.parent, requiredPermissions)

    val internalCreateReportingSetRequest: InternalCreateReportingSetRequest =
      try {
        request.toInternal()
      } catch (e: ServiceException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    return try {
      internalReportingSetsStub
        .createReportingSet(internalCreateReportingSetRequest)
        .toReportingSet()
    } catch (e: StatusException) {
      throw when (ReportingInternalException.getErrorCode(e)) {
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          Status.NOT_FOUND.withCause(e).asRuntimeException()
        ErrorCode.REPORTING_SET_ALREADY_EXISTS ->
          Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
        ErrorCode.REPORTING_SET_NOT_FOUND ->
          Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
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

  /**
   * Validates EventGroups in request [primitive].
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private fun validateEventGroups(primitive: ReportingSet.Primitive) {
    if (primitive.cmmsEventGroupsList.isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("cmms_event_groups is not set")
        .asRuntimeException()
    }
    for (eventGroupName in primitive.cmmsEventGroupsList) {
      if (CmmsEventGroupKey.fromName(eventGroupName) == null) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "$eventGroupName is not a valid EventGroup resource name"
          )
          .asRuntimeException()
      }
    }
  }

  /**
   * Validates a request [setExpression].
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private fun validateSetExpression(
    parentKey: MeasurementConsumerKey,
    setExpression: ReportingSet.SetExpression,
  ) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (setExpression.operation) {
      ReportingSet.SetExpression.Operation.UNION,
      ReportingSet.SetExpression.Operation.DIFFERENCE,
      ReportingSet.SetExpression.Operation.INTERSECTION -> {}
      ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED,
      ReportingSet.SetExpression.Operation.UNRECOGNIZED ->
        throw Status.INVALID_ARGUMENT.withDescription("operation not set or invalid")
          .asRuntimeException()
    }

    validateOperand(parentKey, setExpression.lhs)
    if (setExpression.hasRhs()) {
      validateOperand(parentKey, setExpression.rhs)
    }
  }

  /**
   * Validates a request [operand].
   *
   * @throws io.grpc.StatusRuntimeException
   */
  private fun validateOperand(
    parentKey: MeasurementConsumerKey,
    operand: ReportingSet.SetExpression.Operand,
  ) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET -> {
        val reportingSetKey =
          ReportingSetKey.fromName(operand.reportingSet)
            ?: throw Status.INVALID_ARGUMENT.withDescription(
                "${operand.reportingSet} is not a valid ReportingSet resource name"
              )
              .asRuntimeException()
        if (reportingSetKey.parentKey != parentKey) {
          throw Status.INVALID_ARGUMENT.withDescription("ReportingSet has incorrect parent")
            .asRuntimeException()
        }
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        validateSetExpression(parentKey, operand.expression)
      }
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET ->
        throw Status.INVALID_ARGUMENT.withDescription("operand not set").asRuntimeException()
    }
  }

  override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(request.name)) {
        "ReportingSet name is either unspecified or invalid."
      }
    authorization.check(listOf(request.name, reportingSetKey.parentKey.toName()), Permission.GET)

    val internalResponse =
      try {
        getInternalReportingSet(
          reportingSetKey.toName(),
          reportingSetKey.cmmsMeasurementConsumerId,
          internalReportingSetsStub,
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("${request.name} not found")
            else -> Status.INTERNAL
          }
          .withCause(e)
          .asRuntimeException()
      }

    return internalResponse.toReportingSet()
  }

  override suspend fun listReportingSets(
    request: ListReportingSetsRequest
  ): ListReportingSetsResponse {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }
    val listReportingSetsPageToken = request.toListReportingSetsPageToken()

    authorization.check(request.parent, Permission.LIST)

    val results: List<InternalReportingSet> =
      internalReportingSetsStub
        .streamReportingSets(listReportingSetsPageToken.toStreamReportingSetsRequest())
        .toList()

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

  /**
   * Converts a [CreateReportingSetRequest] to an [InternalCreateReportingSetRequest].
   *
   * @throws InvalidFieldValueException
   * @throws RequiredFieldNotSetException
   * @throws CampaignGroupInvalidException
   */
  private suspend fun CreateReportingSetRequest.toInternal(): InternalCreateReportingSetRequest {
    val source = this
    val cmmsMeasurementConsumerId =
      checkNotNull(MeasurementConsumerKey.fromName(source.parent)).measurementConsumerId

    val internalReportingSet =
      source.reportingSet.toInternal(
        source.reportingSetId,
        cmmsMeasurementConsumerId,
        internalReportingSetsStub,
      )

    return internalCreateReportingSetRequest {
      this.reportingSet = internalReportingSet
      this.externalReportingSetId = source.reportingSetId
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

/** Converts a public [ListReportingSetsRequest] to a [ListReportingSetsPageToken]. */
private fun ListReportingSetsRequest.toListReportingSetsPageToken(): ListReportingSetsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }

  return if (source.pageToken.isNotBlank()) {
    ListReportingSetsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == parentKey.measurementConsumerId) {
        "Arguments must be kept the same when using a page token"
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
