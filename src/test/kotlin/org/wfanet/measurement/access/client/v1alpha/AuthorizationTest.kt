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

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doSuspendableAnswer
import org.mockito.kotlin.stub
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication
import org.wfanet.measurement.access.client.v1alpha.testing.ProtectedResourceMatcher.Companion.hasProtectedResource
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

@RunWith(JUnit4::class)
class AuthorizationTest {
  private val permissionsServiceMock = mockService<PermissionsGrpcKt.PermissionsCoroutineImplBase>()
  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(permissionsServiceMock) }

  private lateinit var authorization: Authorization

  @Before
  fun initAuthorization() {
    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `check does not throw when principal has required permissions on resource`() = runBlocking {
    val book = "books/wind-and-truth"
    val permissionId = "library.books.get"
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasProtectedResource(book)) } doReturn
        checkPermissionsResponse { permissions += "permissions/$permissionId" }
    }

    Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.*")) {
      authorization.check(book, setOf(permissionId))
    }
  }

  @Test
  fun `check does not throw when principal has required permissions on any resource`() =
    runBlocking {
      val book = "books/wind-and-truth"
      val shelf = "shelves/fantasy"
      val permissionId = "library.books.get"
      permissionsServiceMock.stub {
        onBlocking { checkPermissions(hasProtectedResource(book)) } doReturn
          CheckPermissionsResponse.getDefaultInstance()
        onBlocking { checkPermissions(hasProtectedResource(shelf)) } doReturn
          checkPermissionsResponse { permissions += "permissions/$permissionId" }
      }

      Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.*")) {
        authorization.check(listOf(book, shelf), setOf(permissionId))
      }
    }

  @Test
  fun `check returns once principal has required permissions on any resource`() = runBlocking {
    val book = "books/wind-and-truth"
    val shelf = "shelves/fantasy"
    val permissionId = "library.books.get"
    val otherBranchCancellation: CompletableJob = Job()
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasProtectedResource(book)) } doSuspendableAnswer
        {
          // Demonstrate short-circuiting behavior.
          runUntilCancelled(otherBranchCancellation)
        }
      onBlocking { checkPermissions(hasProtectedResource(shelf)) } doReturn
        checkPermissionsResponse { permissions += "permissions/$permissionId" }
    }

    Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.*")) {
      authorization.check(listOf(book, shelf), setOf(permissionId))
    }

    // Verify that the other branch is cancelled.
    otherBranchCancellation.join()
  }

  @Test
  fun `check throws UNAUTHENTICATED when Principal not in context`() = runBlocking {
    val book = "books/wind-and-truth"
    val shelf = "shelves/fantasy"
    val permissionId = "library.books.get"

    val exception =
      assertFailsWith<StatusRuntimeException> {
        authorization.check(listOf(book, shelf), setOf(permissionId))
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `check throws PERMISSION_DENIED when permission not in scopes`() = runBlocking {
    val shelf = "shelves/fantasy"
    val permissionId = "library.books.create"
    permissionsServiceMock.stub {
      onBlocking { checkPermissions(hasProtectedResource(shelf)) } doReturn
        checkPermissionsResponse { permissions += "permissions/$permissionId" }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.get")) {
          authorization.check(shelf, setOf(permissionId))
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `check throws PERMISSION_DENIED when principal does not have all required permissions`() =
    runBlocking {
      val shelf = "shelves/fantasy"
      val permissionId = "library.books.create"
      permissionsServiceMock.stub {
        onBlocking { checkPermissions(hasProtectedResource(shelf)) } doReturn
          checkPermissionsResponse { permissions += "permissions/library.books.list" }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.get")) {
            authorization.check(shelf, setOf(permissionId))
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception).hasMessageThat().contains(shelf)
      assertThat(exception).hasMessageThat().contains(permissionId)
    }

  @Test
  fun `check throws PERMISSION_DENIED when principal does not have required permissions on any resource`() =
    runBlocking {
      val book = "books/wind-and-truth"
      val shelf = "shelves/fantasy"
      val permissionId = "library.books.get"
      permissionsServiceMock.stub {
        onBlocking { checkPermissions(any()) } doReturn
          CheckPermissionsResponse.getDefaultInstance()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          Authentication.withPrincipalAndScopes(PRINCIPAL, setOf("library.books.get")) {
            authorization.check(listOf(book, shelf), permissionId)
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception).hasMessageThat().contains(book)
      assertThat(exception).hasMessageThat().contains(permissionId)
    }

  private suspend fun runUntilCancelled(cancellation: CompletableJob): Nothing {
    while (true) {
      try {
        delay(1000)
      } catch (e: CancellationException) {
        cancellation.complete()
        throw e
      }
    }
  }

  companion object {
    private val PRINCIPAL = principal { name = "principals/user-1" }
  }
}
