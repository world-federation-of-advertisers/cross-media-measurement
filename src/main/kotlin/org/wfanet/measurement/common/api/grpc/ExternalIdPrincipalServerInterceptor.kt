package org.wfanet.measurement.common.api.grpc

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.Principal
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor

class ExternalIdPrincipalServerInterceptor<T : Principal>(
  private val principalContextKey: Context.Key<T>,
  private val externalId: Long,
  private val externalIdPrincipalLookup: PrincipalLookup<T, Long>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : SuspendableServerInterceptor(coroutineContext) {
  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    var rpcContext = Context.current()
    if (principalContextKey.get() != null) {
      return Contexts.interceptCall(rpcContext, call, headers, next)
    }

    val principal: T? = externalIdPrincipalLookup.getPrincipal(externalId)
    if (principal == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No single principal found"), headers)
    } else {
      rpcContext = rpcContext.withValue(principalContextKey, principal)
    }

    return Contexts.interceptCall(rpcContext, call, headers, next)
  }
}
