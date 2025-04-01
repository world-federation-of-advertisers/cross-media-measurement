-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset jojijacob:23 dbms:cloudspanner

-- Adding Cloud Spanner tables for the MetadataStore and MetadataAction. These tables
-- will be used to track actions taken on Resources(i.e. Requisitions, LabeledImpressions, etc.)
--
-- Table hierarchy:
--  Root
--   ├── ResourceMetadata
--   └── MetadataAction
--
START BATCH DDL;

CREATE TABLE ResourceMetadata (
    ResourceMetadataId INT64 NOT NULL,
    ResourceName STRING(MAX),
    StorageLocation STRING(MAX),
    State INT64,
    CreateTime TIMESTAMP NOT NULL,
) PRIMARY KEY (ResourceMetadataId, ResourceName),

CREATE TABLE MetadataAction (
    ActionId INT64 NOT NULL,
    DataProviderName STRING(MAX),
    ActorType INT64,
    ResourceMetaDataId INT64,
    PriorState INT64,
    NewState INT64,
    ActionTime TIMESTAMP NOT NULL,
    CreateTime TIMESTAMP NOT NULL,
    FOREIGN KEY (ResourceMetaDataId) REFERENCES ResourceMetadata(ResourceMetadataId),
) PRIMARY KEY (ActionId, DataProviderName);

RUN BATCH;
