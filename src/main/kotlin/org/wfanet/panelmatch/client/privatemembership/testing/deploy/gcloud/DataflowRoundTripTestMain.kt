// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.privatemembership.testing.deploy.gcloud

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.privatemembership.batch.ParametersKt.shardParameters
import com.google.privatemembership.batch.Shared.EncryptedQueryResult as RlweEncryptedQueryResult
import com.google.privatemembership.batch.Shared.PublicKey
import com.google.privatemembership.batch.client.Client.PrivateKey
import com.google.privatemembership.batch.client.decryptQueriesRequest
import com.google.privatemembership.batch.client.generateKeysRequest
import com.google.privatemembership.batch.parameters
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.random.Random
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.common.databaseEntryOf
import org.wfanet.panelmatch.client.common.encryptedEntryOf
import org.wfanet.panelmatch.client.common.lookupKeyAndIdOf
import org.wfanet.panelmatch.client.common.lookupKeyOf
import org.wfanet.panelmatch.client.common.paddingNonceOf
import org.wfanet.panelmatch.client.privatemembership.BucketContents
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.DatabaseEntry
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryBundle
import org.wfanet.panelmatch.client.privatemembership.EncryptedQueryResult
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembership
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.createQueries
import org.wfanet.panelmatch.client.privatemembership.evaluateQueries
import org.wfanet.panelmatch.client.privatemembership.testing.PRIVATE_MEMBERSHIP_CRYPTO_PARAMETERS
import org.wfanet.panelmatch.common.beam.flatMap
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDo
import org.wfanet.panelmatch.common.beam.parDoWithSideInput
import org.wfanet.panelmatch.common.beam.toMapView
import org.wfanet.panelmatch.common.beam.toSingletonView
import org.wfanet.panelmatch.common.crypto.AsymmetricKeyPair

interface Options : DataflowPipelineOptions {
  @get:Description("Table where results should be written (<project_id>:<dataset_id>.<table_id>)")
  @get:Validation.Required
  var resultsOutputTable: String
}

private const val SHARD_COUNT = 100
private const val BUCKETS_PER_SHARD_COUNT = 2047
private const val QUERIES_PER_SHARD_COUNT = 16

private val PRIVATE_MEMBERSHIP_PARAMETERS = parameters {
  cryptoParameters = PRIVATE_MEMBERSHIP_CRYPTO_PARAMETERS
  shardParameters = shardParameters {
    numberOfBucketsPerShard = BUCKETS_PER_SHARD_COUNT + 1
    numberOfShards = SHARD_COUNT
    requiredQueriesPerShard = QUERIES_PER_SHARD_COUNT
    requiredFakeQueries = 0
  }
}

// TODO: generalize this to other Cloud Providers/Runners.
fun main(args: Array<String>) {
  val options = makeOptions(args)
  FileSystems.setDefaultPipelineOptions(options)

  val pipeline = Pipeline.create(options)

  val privateMembershipKeys =
    pipeline.apply(
      "Create Private Membership Keys",
      Create.ofProvider(
        AsymmetricKeysValueProvider(),
        SerializableCoder.of(AsymmetricKeyPair::class.java),
      ),
    )

  val createQueriesParameters =
    CreateQueriesParameters(
      numShards = SHARD_COUNT,
      numBucketsPerShard = BUCKETS_PER_SHARD_COUNT,
      maxQueriesPerShard = QUERIES_PER_SHARD_COUNT,
      padQueries = true,
    )

  val privateMembershipCryptor =
    JniPrivateMembershipCryptor(Any.pack(PRIVATE_MEMBERSHIP_PARAMETERS))

  val rawQueries: PCollection<LookupKeyAndId> =
    pipeline.apply("Create Queries", Create.of(0 until SHARD_COUNT)).parDo("Populate Queries") { i
      ->
      for (j in 0 until QUERIES_PER_SHARD_COUNT / 4) {
        val queryIndex = i + j * SHARD_COUNT
        yield(lookupKeyAndIdOf(queryIndex.toLong(), "joinKeyId of $queryIndex".toByteStringUtf8()))
      }
    }

  val encryptedQueryBundles: PCollection<EncryptedQueryBundle> =
    createQueries(
        rawQueries,
        privateMembershipKeys.toSingletonView(),
        createQueriesParameters,
        privateMembershipCryptor,
      )
      .encryptedQueryBundles

  val paddingNonces =
    encryptedQueryBundles
      .flatMap { it.queryIdsList }
      .map { kvOf(it, paddingNonceOf("padding-nonce-for-${it.id}".toByteStringUtf8())) }
      .toMapView()

  val queryEvaluator = JniQueryEvaluator(Any.pack(PRIVATE_MEMBERSHIP_PARAMETERS))

  val database: PCollection<DatabaseEntry> =
    pipeline.apply("Create Database Shards", Create.of(0 until SHARD_COUNT)).flatMap(
      "Populate Database"
    ) { i ->
      (0 until BUCKETS_PER_SHARD_COUNT / 2).map { j ->
        val uniqueQueryId = i + j * SHARD_COUNT
        databaseEntryOf(
          lookupKeyOf(Random.nextLong()),
          encryptedEntryOf(makeFakeUserDataPayload(uniqueQueryId.toString())),
        )
      }
    }

  val evaluateQueriesParameters =
    EvaluateQueriesParameters(
      numShards = SHARD_COUNT,
      numBucketsPerShard = BUCKETS_PER_SHARD_COUNT,
      maxQueriesPerShard = QUERIES_PER_SHARD_COUNT,
    )

  val serializedPublicKey = privateMembershipKeys.map { it.serializedPublicKey }.toSingletonView()
  val results: PCollection<EncryptedQueryResult> =
    evaluateQueries(
      database,
      encryptedQueryBundles,
      serializedPublicKey,
      paddingNonces,
      evaluateQueriesParameters,
      queryEvaluator,
    )

  val outputSchema =
    TableSchema()
      .setFields(
        listOf(
          TableFieldSchema().setName("QueryId").setType("INT64"),
          TableFieldSchema().setName("ResultIndex").setType("INT64"),
          TableFieldSchema().setName("Result").setType("STRING"),
        )
      )

  results
    .parDoWithSideInput<EncryptedQueryResult, AsymmetricKeyPair, TableRow>(
      privateMembershipKeys.toSingletonView(),
      name = "Decrypt to TableRows",
    ) { encryptedQueryResult: EncryptedQueryResult, keys: AsymmetricKeyPair ->
      val response =
        JniPrivateMembership.decryptQueries(
          decryptQueriesRequest {
            parameters = PRIVATE_MEMBERSHIP_PARAMETERS
            privateKey = PrivateKey.parseFrom(keys.serializedPrivateKey)
            publicKey = PublicKey.parseFrom(keys.serializedPublicKey)
            encryptedQueries +=
              RlweEncryptedQueryResult.parseFrom(
                encryptedQueryResult.serializedEncryptedQueryResult
              )
          }
        )
      val result = response.resultList.single().result
      if (!result.isEmpty) {
        val bucketContents = BucketContents.parseFrom(result)
        for ((i, item) in bucketContents.itemsList.withIndex()) {
          yield(
            TableRow()
              .set("QueryId", encryptedQueryResult.queryId.id.toLong())
              .set("ResultIndex", i.toLong())
              .set("Result", item.toStringUtf8())
          )
        }
      }
    }
    .toBigQuery(options.resultsOutputTable, outputSchema)

  val pipelineResult = pipeline.run()
  check(pipelineResult.waitUntilFinish() == PipelineResult.State.DONE)
}

private class AsymmetricKeysValueProvider : ValueProvider<AsymmetricKeyPair> {
  private val value by lazy {
    val generateKeysResponse =
      JniPrivateMembership.generateKeys(
        generateKeysRequest { parameters = PRIVATE_MEMBERSHIP_PARAMETERS }
      )
    val publicKey: PublicKey = generateKeysResponse.publicKey
    val privateKey: PrivateKey = generateKeysResponse.privateKey
    AsymmetricKeyPair(
      serializedPublicKey = publicKey.toByteString(),
      serializedPrivateKey = privateKey.toByteString(),
    )
  }

  override fun get(): AsymmetricKeyPair = value

  override fun isAccessible(): Boolean = true
}

private fun PCollection<TableRow>.toBigQuery(outputTable: String, tableSchema: TableSchema) {
  apply(
    "Write to $outputTable",
    BigQueryIO.writeTableRows()
      .to(outputTable)
      .withSchema(tableSchema)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE),
  )
}

private fun makeOptions(args: Array<String>): Options {
  return PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(Options::class.java)
}

private val PAYLOAD_PREFIX = (0 until 333).joinToString("") { "prefix" }

private fun makeFakeUserDataPayload(suffix: String): ByteString {
  return "$PAYLOAD_PREFIX:$suffix".toByteStringUtf8()
}
