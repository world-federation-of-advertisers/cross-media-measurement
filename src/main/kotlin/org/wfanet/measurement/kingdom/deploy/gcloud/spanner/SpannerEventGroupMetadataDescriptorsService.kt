// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupMetadataDescriptorAlreadyExistsWithTypeException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupMetadataDescriptorNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroupMetadataDescriptors
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroupMetadataDescriptor

class SpannerEventGroupMetadataDescriptorsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  knownMetadataTypes: Iterable<Descriptors.FileDescriptor>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupMetadataDescriptorsCoroutineImplBase(coroutineContext) {
  private val allKnownMetadataTypes: Set<Descriptors.FileDescriptor> =
    knownMetadataTypes.asSequence().plus(ProtoReflection.WELL_KNOWN_TYPES).toSet()

  override suspend fun createEventGroupMetadataDescriptor(
    request: EventGroupMetadataDescriptor
  ): EventGroupMetadataDescriptor {
    try {
      return CreateEventGroupMetadataDescriptor(
          request,
          getProtobufTypeNames(request.details.descriptorSet),
        )
        .execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    } catch (e: EventGroupMetadataDescriptorAlreadyExistsWithTypeException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error")
    }
  }

  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    return EventGroupMetadataDescriptorReader()
      .readByExternalIds(
        client.singleUse(),
        request.externalDataProviderId,
        request.externalEventGroupMetadataDescriptorId,
      )
      ?.eventGroupMetadataDescriptor
      ?: throw EventGroupMetadataDescriptorNotFoundException(
          ExternalId(request.externalDataProviderId),
          ExternalId(request.externalEventGroupMetadataDescriptorId),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroupMetadataDescriptor not found.")
  }

  override suspend fun updateEventGroupMetadataDescriptor(
    request: UpdateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    grpcRequire(request.eventGroupMetadataDescriptor.externalDataProviderId > 0L) {
      "ExternalDataProviderId unspecified"
    }
    grpcRequire(request.eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId > 0L) {
      "ExternalEventGroupMetadataDescriptorId unspecified"
    }
    try {
      return UpdateEventGroupMetadataDescriptor(
          request.eventGroupMetadataDescriptor,
          getProtobufTypeNames(request.eventGroupMetadataDescriptor.details.descriptorSet),
        )
        .execute(client, idGenerator)
    } catch (e: EventGroupMetadataDescriptorNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.NOT_FOUND,
        "EventGroupMetadataDescriptor not found.",
      )
    } catch (e: EventGroupMetadataDescriptorAlreadyExistsWithTypeException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error")
    }
  }

  override fun streamEventGroupMetadataDescriptors(
    request: StreamEventGroupMetadataDescriptorsRequest
  ): Flow<EventGroupMetadataDescriptor> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }

    return StreamEventGroupMetadataDescriptors(request.filter, request.limit)
      .execute(client.singleUse())
      .map { it.eventGroupMetadataDescriptor }
  }

  /**
   * Returns the protobuf type names of types defined in [fileDescriptorSet], excluding those
   * defined in [allKnownMetadataTypes].
   */
  private fun getProtobufTypeNames(
    fileDescriptorSet: DescriptorProtos.FileDescriptorSet
  ): List<String> {
    return ProtoReflection.buildDescriptors(listOf(fileDescriptorSet), allKnownMetadataTypes).map {
      it.fullName
    }
  }
}
