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

package org.wfanet.measurement.access.service.internal.testing

import com.google.protobuf.ByteString
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMapKt
import org.wfanet.measurement.config.access.PermissionsConfigKt.permission
import org.wfanet.measurement.config.access.permissionsConfig
import org.wfanet.measurement.config.authorityKeyToPrincipalMap

object TestConfig {
  val AUTHORITY_KEY_IDENTIFIER: ByteString =
    byteStringOf(
      0x7C,
      0xE6,
      0x3F,
      0xEA,
      0x65,
      0xED,
      0x71,
      0x3D,
      0x9E,
      0x59,
      0x79,
      0xA0,
      0xC8,
      0x08,
      0xC9,
      0x57,
      0xAA,
      0xC6,
      0xB1,
      0x6A,
    )

  const val TLS_CLIENT_PROTECTED_RESOURCE_NAME = "shelves/fantasy"
  const val TLS_CLIENT_PRINCIPAL_RESOURCE_ID = "shelves-fantasy"

  val TLS_CLIENT_MAPPING =
    TlsClientPrincipalMapping(
      authorityKeyToPrincipalMap {
        entries +=
          AuthorityKeyToPrincipalMapKt.entry {
            authorityKeyIdentifier = AUTHORITY_KEY_IDENTIFIER
            principalResourceName = TLS_CLIENT_PROTECTED_RESOURCE_NAME
          }
      }
    )

  object PermissionResourceId {
    const val BOOKS_GET = "books.get"
    const val BOOKS_LIST = "books.list"
    const val BOOKS_CREATE = "books.create"
    const val BOOKS_DELETE = "books.delete"
  }

  object ResourceType {
    private const val DOMAIN = "library.googleapis.com"
    const val BOOK = "$DOMAIN/Book"
    const val SHELF = "$DOMAIN/Shelf"
  }

  val PERMISSION_MAPPING =
    PermissionMapping(
      permissionsConfig {
        permissions[PermissionResourceId.BOOKS_GET] = permission {
          protectedResourceTypes += ResourceType.SHELF
          protectedResourceTypes += ResourceType.BOOK
        }
        permissions[PermissionResourceId.BOOKS_DELETE] = permission {
          protectedResourceTypes += ResourceType.SHELF
          protectedResourceTypes += ResourceType.BOOK
        }
        permissions[PermissionResourceId.BOOKS_LIST] = permission {
          protectedResourceTypes += ResourceType.SHELF
        }
        permissions[PermissionResourceId.BOOKS_CREATE] = permission {
          protectedResourceTypes += ResourceType.SHELF
        }
      }
    )
}
