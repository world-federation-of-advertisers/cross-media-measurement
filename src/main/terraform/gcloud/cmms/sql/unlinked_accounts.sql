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
  -- DataProviderResourceId is stored bare (no "dataProviders/" prefix) in
  -- Spanner, so it maps directly onto CmmsDataProvider and the per-EDP row
  -- access predicate CmmsDataProvider = '<resource_id>' matches.
  u.DataProviderResourceId AS CmmsDataProvider,
  u.ClientAccountReferenceId,
  u.Brands,
  u.EventGroupReferenceId,
  u.FirstObservedTime
FROM EXTERNAL_QUERY(
  'projects/${project_id}/locations/${region}/connections/edp-aggregator-conn',
  '''SELECT
    DataProviderResourceId,
    ClientAccountReferenceId,
    Brands,
    EventGroupReferenceId,
    FirstObservedTime
  FROM UnlinkedClientAccounts'''
) u
) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
