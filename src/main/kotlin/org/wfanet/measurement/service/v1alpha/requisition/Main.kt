package org.wfanet.measurement.service.v1alpha.requisition

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.ReadOnlyTransaction
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.TransactionManager
import com.google.cloud.spanner.TransactionRunner
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.CommonServerType
import org.wfanet.measurement.db.kingdom.gcp.GcpMeasurementProviderStorage
import java.lang.UnsupportedOperationException
import java.time.Clock

fun main() {
  CommonServer(CommonServerType.REQUISITION,
               RequisitionService(GcpMeasurementProviderStorage(object : DatabaseClient {
                 override fun write(var1: Iterable<Mutation?>?): Timestamp? =
                   throw UnsupportedOperationException()

                 override fun writeAtLeastOnce(var1: Iterable<Mutation?>?): Timestamp? =
                   throw UnsupportedOperationException()

                 override fun singleUse(): ReadContext? = throw UnsupportedOperationException()

                 override fun singleUse(var1: TimestampBound?): ReadContext? =
                   throw UnsupportedOperationException()

                 override fun singleUseReadOnlyTransaction(): ReadOnlyTransaction? =
                   throw UnsupportedOperationException()

                 override fun singleUseReadOnlyTransaction(
                   var1: TimestampBound?
                 ): ReadOnlyTransaction? = throw UnsupportedOperationException()

                 override fun readOnlyTransaction(): ReadOnlyTransaction? =
                   throw UnsupportedOperationException()

                 override fun readOnlyTransaction(var1: TimestampBound?): ReadOnlyTransaction? =
                   throw UnsupportedOperationException()

                 override fun readWriteTransaction(): TransactionRunner? =
                   throw UnsupportedOperationException()

                 override fun transactionManager(): TransactionManager? =
                   throw UnsupportedOperationException()

                 override fun executePartitionedUpdate(var1: Statement?): Long =
                   throw UnsupportedOperationException()
               }, Clock.systemUTC())))
    .start()
    .blockUntilShutdown()
}
