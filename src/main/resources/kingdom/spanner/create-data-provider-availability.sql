-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset sanjayvas:24 dbms:cloudspanner
-- comment: Add DataProviderAvailabilityIntervals table.

CREATE TABLE DataProviderAvailabilityIntervals (
  DataProviderId INT64 NOT NULL,
  ModelProviderId INT64 NOT NULL,
  ModelSuiteId INT64 NOT NULL,
  ModelLineId INT64 NOT NULL,

  StartTime TIMESTAMP NOT NULL,
  EndTime TIMESTAMP NOT NULL,

  FOREIGN KEY (DataProviderId) REFERENCES DataProviders (DataProviderId),
  FOREIGN KEY (ModelProviderId, ModelSuiteId, ModelLineId)
    REFERENCES ModelLines (ModelProviderId, ModelSuiteId, ModelLineId),
)
PRIMARY KEY (DataProviderId, ModelProviderId, ModelSuiteId, ModelLineId),
INTERLEAVE IN PARENT DataProviders;
