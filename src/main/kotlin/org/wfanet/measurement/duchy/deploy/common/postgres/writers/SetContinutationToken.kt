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

package org.wfanet.measurement.duchy.deploy.common.postgres.writers

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.Timestamps
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ContinuationTokenReader
import org.wfanet.measurement.duchy.service.internal.ContinuationTokenInvalidException
import org.wfanet.measurement.duchy.service.internal.ContinuationTokenMalformedException
import org.wfanet.measurement.duchy.service.internal.DuchyInternalException
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsContinuationToken

/**
 * [PostgresWriter] for setting continuation tokens.
 *
 * Throws a subclass of [DuchyInternalException] on [execute]:
 * * [ContinuationTokenMalformedException] when the new token is malformed
 * * [ContinuationTokenInvalidException] when the new token is invalid
 */
class SetContinuationToken(private val continuationToken: String) : PostgresWriter<Unit>() {
  private fun decodeContinuationToken(token: String): StreamActiveComputationsContinuationToken =
    try {
      StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())
    } catch (e: InvalidProtocolBufferException) {
      throw ContinuationTokenMalformedException(
        continuationToken,
        "ContinuationToken is malformed.",
      )
    }

  override suspend fun TransactionScope.runTransaction() {
    val statement =
      boundStatement(
        """
      INSERT INTO HeraldContinuationTokens (Presence, ContinuationToken, UpdateTime)
        VALUES ($1, $2, $3)
      ON CONFLICT (Presence)
      DO
        UPDATE SET ContinuationToken = EXCLUDED.ContinuationToken, UpdateTime = EXCLUDED.UpdateTime;
      """
      ) {
        bind("$1", true)
        bind("$2", continuationToken)
        bind("$3", Instant.now().truncatedTo(ChronoUnit.MICROS))
      }

    transactionContext.run {
      val newContinuationToken = decodeContinuationToken(continuationToken)
      val oldContinuationToken =
        ContinuationTokenReader().getContinuationToken(transactionContext)?.continuationToken?.let {
          decodeContinuationToken(it)
        }

      if (
        oldContinuationToken != null &&
          Timestamps.compare(
            newContinuationToken.lastSeenUpdateTime,
            oldContinuationToken.lastSeenUpdateTime,
          ) < 0
      ) {
        throw ContinuationTokenInvalidException(
          continuationToken,
          "ContinuationToken to set cannot have older timestamp.",
        )
      }
      transactionContext.executeStatement(statement)
    }
  }
}
