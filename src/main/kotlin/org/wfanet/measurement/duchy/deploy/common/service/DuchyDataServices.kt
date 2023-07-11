package org.wfanet.measurement.duchy.deploy.common.service

import io.grpc.BindableService
import kotlin.reflect.full.declaredMemberProperties
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase

data class DuchyDataServices(
  val computationsService: ComputationsCoroutineImplBase,
  val computationStatsService: ComputationStatsCoroutineImplBase,
  val continuationTokensService: ContinuationTokensCoroutineImplBase
)

fun DuchyDataServices.toList(): List<BindableService> {
  return DuchyDataServices::class.declaredMemberProperties.map { it.get(this) as BindableService }
}
