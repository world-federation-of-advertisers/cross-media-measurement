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

-- changeset sanjayvas:30 dbms:cloudspanner
-- comment: Swap EventGroupsNew in for EventGroups.
START BATCH DDL;

DROP INDEX EventGroupsByCreateRequestId;
DROP INDEX EventGroupsByExternalId;
DROP INDEX EventGroupsByMeasurementConsumer;
DROP INDEX EventGroupsByMetadata;

RENAME TABLE
  EventGroups TO EventGroupsOld,
  EventGroupsNew TO EventGroups,
  EventGroupMediaTypes TO EventGroupMediaTypesOld,
  EventGroupMediaTypesNew TO EventGroupMediaTypes;

-- Add foreign keys after rename to work around https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/251
ALTER TABLE EventGroups
  ADD CONSTRAINT FKey_EventGroups_MeasurementConsumers
  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers(MeasurementConsumerId);
ALTER TABLE EventGroups
  ADD CONSTRAINT FKey_EventGroups_MeasurementConsumerCertificates
  FOREIGN KEY (MeasurementConsumerId, MeasurementConsumerCertificateId)
  REFERENCES MeasurementConsumerCertificates(MeasurementConsumerId, CertificateId);

CREATE UNIQUE NULL_FILTERED INDEX EventGroupsByCreateRequestId ON EventGroups(DataProviderId, CreateRequestId);
CREATE UNIQUE INDEX EventGroupsByExternalId ON EventGroups(DataProviderId, ExternalEventGroupId);
CREATE UNIQUE INDEX EventGroupsByMeasurementConsumer ON EventGroups(MeasurementConsumerId, ExternalEventGroupId);
CREATE SEARCH INDEX EventGroupsByMetadata ON EventGroups(Metadata_Tokens);

RUN BATCH;