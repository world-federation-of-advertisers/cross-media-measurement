/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.client.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import kotlin.time.Duration
import kotlin.time.TimeSource
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.whileSelect
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.wfanet.measurement.access.client.ValueInScope
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.checkPermissionsRequest
import org.wfanet.measurement.common.Instrumentation

class Authorization(private val permissionsStub: PermissionsGrpcKt.PermissionsCoroutineStub) {
  private val checkDurationHistogram =
    Instrumentation.meter
      .histogramBuilder("$INSTRUMENTATION_NAMESPACE.authorization_check.duration")
      .setUnit("s")
      .setDescription("Duration of authorization check")
      .build()

  /**
   * Checks whether the [Principal] has all of the [requiredPermissionIds] on any of the specified
   * protected resources.
   *
   * @param protectedResourceNames names of the protected resources, in order of most to least
   *   specific.
   * @param requiredPermissionIds the IDs of the set of Permissions required for the operation on a
   *   protected resource
   * @throws io.grpc.StatusRuntimeException
   */
  suspend fun check(
    protectedResourceNames: Collection<String>,
    requiredPermissionIds: Set<String>,
  ) {
    require(protectedResourceNames.isNotEmpty())

    // TODO(@SanjayVas): Add tracing for this method once Instrumentation exposes a Tracer.
    val timeMark = TimeSource.Monotonic.markNow()
    var statusCode = Status.Code.OK

    try {
      val principal: Principal =
        ContextKeys.PRINCIPAL.get()
          ?: throw Status.UNAUTHENTICATED.withDescription("Principal not found")
            .asRuntimeException()

      // Optimization.
      if (requiredPermissionIds.isEmpty()) {
        return
      }

      val primaryProtectedResourceName = protectedResourceNames.first()
      checkScopes(primaryProtectedResourceName, requiredPermissionIds)

      // Optimization.
      if (protectedResourceNames.size == 1) {
        val response: CheckPermissionsResponse =
          checkPermissions(principal, primaryProtectedResourceName, requiredPermissionIds)
        if (response.permissionsCount != requiredPermissionIds.size) {
          val missingPermissionId: String = getMissingPermissionId(response, requiredPermissionIds)
          throw buildPermissionDeniedException(primaryProtectedResourceName, missingPermissionId)
        }
        return
      }

      checkPermissionsParallel(protectedResourceNames, principal, requiredPermissionIds)
        .getOrThrow()
    } catch (e: StatusRuntimeException) {
      statusCode = e.status.code
      throw e
    } catch (e: Exception) {
      statusCode = Status.Code.UNKNOWN
      throw e
    } finally {
      val duration: Duration = timeMark.elapsedNow()
      checkDurationHistogram.record(
        duration.inWholeMilliseconds / 1000.0,
        Attributes.of(
          PROTECTED_RESOURCE_COUNT_ATTRIBUTE,
          protectedResourceNames.size.toLong(),
          GRPC_STATUS_CODE_ATTRIBUTE,
          statusCode.value().toLong(),
        ),
      )
    }
  }

  /**
   * Calls checkPermissions in parallel for each protected resource, short-circuiting on a result
   * where [principal] has all the required permissions.
   *
   * @return the result of the parallel calls, where success means the principal has all required
   *   permissions.
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `whileSelect`.
  private suspend fun checkPermissionsParallel(
    protectedResourceNames: Collection<String>,
    principal: Principal,
    requiredPermissionIds: Set<String>,
  ): Result<Unit> = coroutineScope {
    val primaryProtectedResourceName = protectedResourceNames.first()
    var result: Result<Unit>? = null

    val responseByProtectedResource = mutableMapOf<String, Deferred<CheckPermissionsResponse>>()
    protectedResourceNames.associateWithTo(responseByProtectedResource) { protectedResourceName ->
      async { checkPermissions(principal, protectedResourceName, requiredPermissionIds) }
    }

    val mutex = Mutex() // Guard access to mutable fields.
    whileSelect {
      for ((protectedResourceName, deferredResponse) in responseByProtectedResource.toMap()) {
        deferredResponse.onAwait { response ->
          mutex.withLock {
            @Suppress("DeferredResultUnused") // The value is used elsewhere.
            responseByProtectedResource.remove(protectedResourceName)
            if (response.permissionsCount == requiredPermissionIds.size) {
              result = Result.success(Unit)
              false
            } else {
              if (result == null && protectedResourceName == primaryProtectedResourceName) {
                val missingPermissionId: String =
                  getMissingPermissionId(response, requiredPermissionIds)
                result =
                  Result.failure(
                    buildPermissionDeniedException(protectedResourceName, missingPermissionId)
                  )
              }

              responseByProtectedResource.isNotEmpty()
            }
          }
        }
      }
    }

    coroutineContext.cancelChildren()
    result!!
  }

  private fun getMissingPermissionId(
    response: CheckPermissionsResponse,
    requiredPermissionIds: Set<String>,
  ): String {
    val permissionIds: Set<String> =
      response.permissionsList.map { checkNotNull(PermissionKey.fromName(it)).permissionId }.toSet()
    return requiredPermissionIds.first { it !in permissionIds }
  }

  private suspend fun checkPermissions(
    principal: Principal,
    protectedResourceName: String,
    requiredPermissionIds: Set<String>,
  ): CheckPermissionsResponse {
    return try {
      permissionsStub.checkPermissions(
        checkPermissionsRequest {
          this.principal = principal.name
          protectedResource = protectedResourceName
          permissions += requiredPermissionIds.map { PermissionKey(it).toName() }
        }
      )
    } catch (e: StatusException) {
      throw Status.INTERNAL.withCause(e).asRuntimeException()
    }
  }

  private fun checkScopes(protectedResourceName: String, requiredPermissionIds: Set<String>) {
    val scopes: Set<String> = ContextKeys.SCOPES.get() ?: emptySet()
    val notInScope = ValueInScope(scopes).negate()::test

    val missingPermissionId = requiredPermissionIds.find(notInScope)
    if (missingPermissionId != null) {
      throw buildPermissionDeniedException(protectedResourceName, missingPermissionId)
    }
  }

  companion object {
    /** Resource name used to indicate the API root. */
    const val ROOT_RESOURCE_NAME = ""

    private const val ACCESS_INSTRUMENTATION_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.access"
    private const val INSTRUMENTATION_NAMESPACE = "$ACCESS_INSTRUMENTATION_NAMESPACE.client"
    private val PROTECTED_RESOURCE_COUNT_ATTRIBUTE =
      AttributeKey.longKey("$ACCESS_INSTRUMENTATION_NAMESPACE.protected_resource.count")
    private val GRPC_STATUS_CODE_ATTRIBUTE = AttributeKey.longKey("rpc.grpc.status.code")

    /**
     * Builds an exception with [Status.Code.PERMISSION_DENIED].
     *
     * TODO(@SanjayVas): Take in all missing permissions IDs, not just one.
     */
    private fun buildPermissionDeniedException(
      protectedResourceName: String,
      missingPermissionId: String,
    ): StatusRuntimeException {
      return Status.PERMISSION_DENIED.withDescription(
          "Permission $missingPermissionId denied on resource $protectedResourceName " +
            "(or it might not exist)"
        )
        .asRuntimeException()
    }
  }
}

/**
 * Checks whether the [Principal] has all of the [requiredPermissionIds] on the protected resource.
 *
 * @param protectedResourceName name of the protected resource.
 * @param requiredPermissionIds the IDs of the set of Permissions required for the operation on the
 *   protected resource
 * @throws io.grpc.StatusRuntimeException
 */
suspend fun Authorization.check(protectedResourceName: String, requiredPermissionIds: Set<String>) =
  check(listOf(protectedResourceName), requiredPermissionIds)

/**
 * Checks whether the [Principal] has all of the [requiredPermissionIds] on the protected resource.
 *
 * @param protectedResourceNames names of the protected resources, in order of most to least
 *   specific.
 * @param requiredPermissionIds the IDs of the set of Permissions required for the operation on the
 *   protected resource
 * @throws io.grpc.StatusRuntimeException
 */
suspend fun Authorization.check(
  protectedResourceNames: Collection<String>,
  vararg requiredPermissionIds: String,
) = check(protectedResourceNames, requiredPermissionIds.toSet())

/**
 * Checks whether the [Principal] has all of the [requiredPermissionIds] on the protected resource.
 *
 * @param protectedResourceName name of the protected resource.
 * @param requiredPermissionIds the IDs of the set of Permissions required for the operation on the
 *   protected resource
 * @throws io.grpc.StatusRuntimeException
 */
suspend fun Authorization.check(
  protectedResourceName: String,
  vararg requiredPermissionIds: String,
) = check(listOf(protectedResourceName), requiredPermissionIds.toSet())
