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

-- changeset sanjayvas:28 dbms:cloudspanner
-- comment: Add a copy of the EventGroups table that is not interleaved.

CREATE TABLE EventGroupsNew (
  DataProviderId INT64 NOT NULL,
  EventGroupId INT64 NOT NULL,
  MeasurementConsumerId INT64 NOT NULL,
  MeasurementConsumerCertificateId INT64,
  ExternalEventGroupId INT64 NOT NULL,
  ProvidedEventGroupId STRING(MAX),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  EventGroupDetails wfa.measurement.internal.kingdom.EventGroupDetails,
  State INT64 NOT NULL,
  CreateRequestId STRING(MAX),
  DataAvailabilityStartTime TIMESTAMP,
  DataAvailabilityEndTime TIMESTAMP,
  BrandName_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(EventGroupDetails.metadata.ad_metadata.campaign_metadata.brand_name)) HIDDEN,
  CampaignName_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(EventGroupDetails.metadata.ad_metadata.campaign_metadata.campaign_name)) HIDDEN,
  Metadata_Tokens TOKENLIST AS (TOKENLIST_CONCAT([BrandName_Tokens, CampaignName_Tokens])) HIDDEN,
) PRIMARY KEY(DataProviderId, EventGroupId);

CREATE TABLE EventGroupMediaTypesNew (
  DataProviderId INT64 NOT NULL,
  EventGroupId INT64 NOT NULL,
  MediaType INT64 NOT NULL,
) PRIMARY KEY(DataProviderId, EventGroupId, MediaType),
  INTERLEAVE IN PARENT EventGroupsNew ON DELETE CASCADE;
