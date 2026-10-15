-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

MERGE INTO `${project_id}.${dataset}.${table_name}` T
USING (
SELECT
  `${project_id}.dashboard.externalIdToApiId`(dp.ExternalDataProviderId) AS CmmsDataProvider,
  u.ClientAccountReferenceId,
  u.BrandName,
  COALESCE(u.EventGroupReferenceId, CONCAT(u.EventGroupEntityKeyType, '/', u.EventGroupEntityKeyId)) AS ObservedEventGroup,
  u.CreateTime
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      u.DataProviderId,
      u.ClientAccountReferenceId,
      -- BrandName is best-effort: entity_metadata uses each EDP's own schema, so it may be NULL.
      JSON_VALUE(TO_JSON(u.EntityMetadata), '$.brand_name') AS BrandName,
      u.EventGroupReferenceId,
      u.EventGroupEntityKeyType,
      u.EventGroupEntityKeyId,
      u.CreateTime
    FROM UnlinkedClientAccounts u''')
) u
INNER JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      dp.DataProviderId,
      dp.ExternalDataProviderId
    FROM DataProviders dp''')
) dp
  ON u.DataProviderId = dp.DataProviderId
) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
