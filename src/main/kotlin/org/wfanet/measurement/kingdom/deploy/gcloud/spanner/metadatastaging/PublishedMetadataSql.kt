// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

/** Shared visibility rules for resource reads and indexed metadata searches. */
object PublishedMetadataSql {
  val visibleVersions =
    """
    WITH PublishedVersions AS (
      SELECT O.DataProviderId, O.MeasurementConsumerId, S.EventGroupId,
        S.OperationId, S.Metadata, O.PublicationSequence
      FROM EventGroupMetadataVersions AS S
      JOIN EventGroupMetadataOperations AS O USING (OperationId)
      JOIN EventGroups AS E ON E.DataProviderId = O.DataProviderId
        AND E.MeasurementConsumerId = O.MeasurementConsumerId
        AND E.EventGroupId = S.EventGroupId
      WHERE O.PublicationSequence > E.EntityMetadataPublicationSequence
    ), LatestVersions AS (
      SELECT DataProviderId, MeasurementConsumerId, EventGroupId,
        MAX(PublicationSequence) AS PublicationSequence
      FROM PublishedVersions GROUP BY DataProviderId, MeasurementConsumerId, EventGroupId
    ), VisibleVersions AS (
      SELECT P.* FROM PublishedVersions AS P
      JOIN LatestVersions USING (DataProviderId, MeasurementConsumerId, EventGroupId, PublicationSequence)
    )
    """
      .trimIndent()

  fun searchPredicate(parameter: String): String =
    """
    EXISTS (
      SELECT 1 FROM (
      SELECT E.DataProviderId, E.EventGroupId
      FROM EventGroups@{FORCE_INDEX=EventGroupsByEntityMetadata} AS E
      WHERE SEARCH(E.Metadata_Tokens, @$parameter) OR SEARCH(E.EntityType_Tokens, @$parameter)
      UNION ALL
      SELECT E.DataProviderId, E.EventGroupId
      FROM EventGroups@{FORCE_INDEX=EventGroupsByEntityMetadata} AS E
      LEFT JOIN VisibleVersions AS V USING (DataProviderId, MeasurementConsumerId, EventGroupId)
      WHERE V.OperationId IS NULL AND SEARCH(E.EntityMetadata_Tokens, @$parameter)
      UNION ALL
      SELECT V.DataProviderId, V.EventGroupId
      FROM EventGroupMetadataVersions@{FORCE_INDEX=EventGroupMetadataVersionsByValues} AS S
      JOIN VisibleVersions AS V USING (OperationId, EventGroupId)
      WHERE SEARCH(S.Metadata_Tokens, @$parameter)
      ) AS Matches
      WHERE Matches.DataProviderId = EventGroups.DataProviderId
        AND Matches.EventGroupId = EventGroups.EventGroupId
    )
    """
      .trimIndent()
}
