-- liquibase formatted sql

-- Copyright 2023 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset marcopremier:6 dbms:cloudspanner
CREATE TABLE DataProviderRequiredDuchies (
    DataProviderId INT64 NOT NULL,
    -- Duchy internal id that must be included in every computation that involves this DP id.
    DuchyId INT64 NOT NULL,
) PRIMARY KEY (DataProviderId, DuchyId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;
