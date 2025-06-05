/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.privacybudgetmanager

import com.google.protobuf.Descriptors
import org.wfanet.measurement.privacybudgetmanager.LandscapeProcessor.MappingNode

/**
 * Instantiates a privacy budget manager.
 *
 * @param auditLog: An object that transactionally logs the charged [Query]s to an EDP owned log.
 * @param landscapeMappingChain: A list of [MappingNode] that is used to map older landscapes to the
 *   acitve landscape. The tail of this list has to be the active landscape currently in use by the
 *   ledger.
 * @param ledger: A [Ledger] object where the [PrivacyCharge]s and [Query]s are stored
 * @param maximumPrivacyBudget: The maximum privacy budget that can be used in any privacy bucket.
 * @param maximumTotalDelta: Maximum total value of the delta parameter that can be used in any
 *   privacy bucket.
 *
 * TODO(uakyol): Validate the Mapping nodes and their constituents
 */
class PrivacyBudgetManager(
  private val auditLog: AuditLog,
  private val landscapeMappingChain: List<MappingNode>,
  private val ledger: Ledger,
  private val landscapeProcessor: LandscapeProcessor,
  private val maximumPrivacyBudget: Float,
  private val maximumTotalDelta: Float,
  private val eventTemplateDescriptor: Descriptors.Descriptor,
) {

  /**
   * Charges the PBM in batch with the charges resulting from the given queries and writes the
   * successful charge operation to the audit log.
   *
   * @throws PrivacyBudgetManager exception if either the charging or audit log write operations are
   *   unsuccessful.
   */
  suspend fun charge(queries: List<Query>, groupId: String): String {
    // Holds the quries written to the ledger with their commit times read from the ledger.
    val queriesWithCommitTime: List<Query>

    ledger.startTransaction().use { context: TransactionContext ->
      val alreadyCommitedQueries: List<Query> = context.readQueries(queries)
      val alreadyCommittedReferenceIds =
        alreadyCommitedQueries.map { it.queryIdentifiers.externalReferenceId }
      // Filter out the queries that were already committed before.
      val queriesToCommit =
        queries.filter { query ->
          !alreadyCommittedReferenceIds.contains(query.queryIdentifiers.externalReferenceId)
        }

      // Get slice intended to be committed to the PBM, defined by queriesToCommit.
      val delta: Slice = getDelta(queriesToCommit)

      // Read the backing store for the rows that slice targets.
      val targettedSlice = context.readChargeRows(delta.getLedgerRowKeys())

      // Check if any of the updated buckets exceed the budget and aggregate to find the slice
      // to commit.
      val sliceToCommit = checkAndAggregate(delta, targettedSlice)

      // Write aggregated slice and the selected Queries to the Backing Store.
      queriesWithCommitTime = context.write(sliceToCommit, queriesToCommit) + alreadyCommitedQueries

      // Commit the transaction
      context.commit()
    }

    // Write all the given queries to the EDP owned audit log and return the audit reference.
    // The detail of writing all the given queries and not the queriesToCommit is important
    // due to the following scenario:
    //  1. PBM is charged with queries A,B,C -> charges are committed successfully but
    //     the audit log write failed - so the audit log doesn't contain A,B,C. The caller
    // shouldn't
    //     fulfill requisitions for A,B,C.
    //  2. PBM is then called at a later time with queries B,C,D -> charges are committed
    // succesfully
    //     for only D, PBM did not commit charges from B,C because they are already in the
    // backing store.
    //     In this case, all B,C,D should be written to the audit log because if you only write
    // D, then
    //     the auditor will conclude less queries are wrritten to the audit log than in the PBM.
    //
    // This brings about the following scenario:
    //  1. PBM is charged with A,B,C all commited, all successfully written to audit log.
    //  2. Then, PBM is charged with B,C,D. Again, D is committed, all B,C,D written
    // successfully
    //     to audit log.
    // In this scenario, auditor will find duplicate entries for B and C. This won't be a
    // problem
    // because in the replay scenario, the PBM code will check the references and conclude they
    // were
    // already written to the DB.
    return auditLog.write(queriesWithCommitTime, groupId)
  }

  /**
   * Creates the [Slice] that the union of all the given [queries] specify.
   *
   * @throws IllegalStateException exception if there is no mapping for any one of the [queries]
   *   from the inactive landscape it targets to the active landscape
   */
  private suspend fun getDelta(queries: List<Query>): Slice {
    val delta = Slice()
    for (query in queries) {
      delta.add(
        processBucketsForLandscape(
          query.queryIdentifiers.eventDataProviderId,
          query.queryIdentifiers.measurementConsumerId,
          query.privacyLandscapeIdentifier,
          query.eventGroupLandscapeMasksList,
        ),
        query.acdpCharge,
      )
    }

    return delta
  }

  /**
   * Finds a PrivacyLandscape by name in the linked list, gets its buckets, and then iteratively
   * maps those buckets to the PrivacyLandscape at the tail of the linked list.
   *
   * @param inactivelandscapeIdentifier The name of the PrivacyLandscape to find initially.
   * @param mappingNodes The linked list of MappingNode.
   * @return The list of PrivacyBuckets mapped to the tail PrivacyLandscape.
   */
  fun processBucketsForLandscape(
    eventDataProviderId: String,
    measurementConsumerId: String,
    inactivelandscapeIdentifier: String,
    eventGroupLandscapeMasks: List<EventGroupLandscapeMask>,
  ): List<PrivacyBucket> {
    val initialNode =
      landscapeMappingChain.find { it.source.landscapeIdentifier == inactivelandscapeIdentifier }
        ?: throw IllegalStateException(
          "Inactive Privacy landscape with name '$inactivelandscapeIdentifier' not found."
        )

    val initialLandscape = initialNode.source
    val initialBuckets =
      landscapeProcessor.getBuckets(
        eventDataProviderId,
        measurementConsumerId,
        eventGroupLandscapeMasks,
        initialLandscape,
        eventTemplateDescriptor,
      )

    var currentBuckets = initialBuckets
    var currentLandscape = initialLandscape

    val iterator = landscapeMappingChain.iterator()
    var currentNode = initialNode
    while (iterator.hasNext()) {
      val nextNode = iterator.next()

      if (
        currentNode.mapping == null ||
          (nextNode.source.landscapeIdentifier != currentNode.mapping.targetLandscape)
      ) {
        throw IllegalStateException("Privacy landscape mapping is illegal")
      }
      val toLandscape = nextNode.source
      currentBuckets =
        landscapeProcessor.mapBuckets(
          currentBuckets,
          currentNode.mapping,
          currentLandscape,
          toLandscape,
        )
      currentLandscape = toLandscape
      currentNode = nextNode
    }

    // After iterating through the relevant mappings, currentBuckets will hold the
    // buckets mapped to the tail of the linked list (the last toPrivacyLandscape
    // encountered in the chain).
    return currentBuckets
  }

  /**
   * Creates the Slice that will be commited to the [ledger]. Does this by aggregating the charges
   * from the Slice in ledger - [targettedSlice] and slice specified by the given queries [delta].
   * All the buckets in this slice are checked to be within the privacy budget.
   *
   * @throws PrivacyBudgetManagerException if any of the buckets in the slice exceed the budget
   */
  private suspend fun checkAndAggregate(delta: Slice, targettedSlice: Slice): Slice =
    TODO("uakyol: implement this")
}
