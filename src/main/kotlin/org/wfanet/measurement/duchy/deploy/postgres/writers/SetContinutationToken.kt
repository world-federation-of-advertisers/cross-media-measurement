package org.wfanet.measurement.duchy.deploy.postgres.writers

import com.google.protobuf.util.Timestamps
import java.time.Instant
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.deploy.postgres.readers.ContinuationTokenReader
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsContinuationToken

class SetContinuationToken(private val continuationToken: String) : PostgresWriter<Unit>() {
  private fun decodeContinuationToken(token: String): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(token.base64UrlDecode())

  override suspend fun TransactionScope.runTransaction() {
    val statement = boundStatement(
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
      bind("$3", Instant.now())
    }

    transactionContext.run {
      val newContinuationToken = decodeContinuationToken(continuationToken)
      val oldContinuationToken =
        ContinuationTokenReader()
          .getContinuationToken(transactionContext)?.continuationToken?.let {
            decodeContinuationToken(it)
          }

      if (oldContinuationToken != null && Timestamps.compare(
          newContinuationToken.updateTimeSince, oldContinuationToken.updateTimeSince
        ) < 0
      ) {
        throw InvalidContinuationTokenException(
          "ContinuationToken to set cannot have older timestamp."
        )
      }
      transactionContext.executeStatement(statement)
    }
  }
}

class InvalidContinuationTokenException(message: String) : Exception(message) {}
