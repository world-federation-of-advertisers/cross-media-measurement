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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader

class StreamCertificates(
  parentType: CertificateReader.ParentType,
  requestFilter: StreamCertificatesRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<CertificateReader.Result>() {
  override val reader =
    CertificateReader(parentType).fillStatementBuilder {
      bindWhereClause(parentType, requestFilter)
      appendOrderByClause(parentType)
      if (limit > 0) {
        bind(Params.LIMIT).to(limit.toLong())
        appendClause("LIMIT @${Params.LIMIT}")
      }
    }

  private object Params {
    const val LIMIT = "limit"
    const val EXTERNAL_PARENT_ID = "externalParentId"
    const val EXTERNAL_CERTIFICATE_ID = "externalCertificateId"
    const val NOT_VALID_BEFORE = "notValidBefore"
    const val SUBJECT_KEY_IDENTIFIERS = "subjectKeyIdentifiers"
  }

  companion object {
    private fun Statement.Builder.appendOrderByClause(parentType: CertificateReader.ParentType) {
      val expressions =
        mutableListOf("NotValidBefore DESC", parentType.externalCertificateIdColumnName)
      val externalParentIdColumn: String? = parentType.externalIdColumnName
      if (externalParentIdColumn != null) {
        expressions.add(externalParentIdColumn)
      }

      appendClause("ORDER BY " + expressions.joinToString(", "))
    }

    private fun Statement.Builder.bindWhereClause(
      parentType: CertificateReader.ParentType,
      requestFilter: StreamCertificatesRequest.Filter,
    ) {
      val conjuncts = mutableListOf<String>()
      if (requestFilter.hasAfter()) {
        val externalCertificateIdColumn = parentType.externalCertificateIdColumnName
        val externalParentIdColumn: String? = parentType.externalIdColumnName
        val conjunct =
          if (externalParentIdColumn == null) {
            """
            NotValidBefore < @${Params.NOT_VALID_BEFORE} OR (
              NotValidBefore = @${Params.NOT_VALID_BEFORE} AND
                $externalCertificateIdColumn > @${Params.EXTERNAL_CERTIFICATE_ID}
            )
            """
              .trimIndent()
          } else {
            bind(Params.EXTERNAL_PARENT_ID to requestFilter.getExternalParentId(parentType))

            """
            NotValidBefore < @${Params.NOT_VALID_BEFORE} OR (
              NotValidBefore = @${Params.NOT_VALID_BEFORE} AND (
                $externalCertificateIdColumn > @${Params.EXTERNAL_CERTIFICATE_ID} OR (
                  $externalCertificateIdColumn = @${Params.EXTERNAL_CERTIFICATE_ID} AND
                    $externalParentIdColumn > @${Params.EXTERNAL_PARENT_ID}
                )
              )
            )
            """
              .trimIndent()
          }

        bind(Params.NOT_VALID_BEFORE).to(requestFilter.after.notValidBefore.toGcloudTimestamp())
        bind(Params.EXTERNAL_CERTIFICATE_ID).to(requestFilter.after.externalCertificateId)
        conjuncts.add(conjunct)
      }
      if (requestFilter.subjectKeyIdentifiersList.isNotEmpty()) {
        bind(Params.SUBJECT_KEY_IDENTIFIERS)
          .toBytesArray(requestFilter.subjectKeyIdentifiersList.map { it.toGcloudByteArray() })
        conjuncts.add("SubjectKeyIdentifier IN UNNEST(@${Params.SUBJECT_KEY_IDENTIFIERS})")
      }

      if (conjuncts.isEmpty()) {
        return
      }

      appendClause("WHERE ")
      append(conjuncts.joinToString(" AND "))
    }

    private fun StreamCertificatesRequest.Filter.getExternalParentId(
      parentType: CertificateReader.ParentType
    ): ExternalId {
      val value: Long =
        when (parentType) {
          CertificateReader.ParentType.DATA_PROVIDER -> externalDataProviderId
          CertificateReader.ParentType.MEASUREMENT_CONSUMER -> externalMeasurementConsumerId
          CertificateReader.ParentType.MODEL_PROVIDER -> externalModelProviderId
          CertificateReader.ParentType.DUCHY ->
            error("ParentType $parentType does not have an integer external ID")
        }
      return ExternalId(value)
    }
  }
}
