-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
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

-- changeset stevenwarejones:1 dbms:cloudspanner

START BATCH DDL;

-- Transactional outbox for publishing WorkItems to their configured queues. A row exists while
-- publication is pending. Leases allow multiple control-plane replicas to publish safely.
CREATE TABLE WorkItemPublications (
    WorkItemId INT64 NOT NULL,

    LeaseOwner STRING(36),
    LeaseExpirationTime TIMESTAMP,
    AttemptCount INT64 NOT NULL,

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

) PRIMARY KEY (WorkItemId),
    INTERLEAVE IN PARENT WorkItems ON DELETE CASCADE;

CREATE INDEX WorkItemPublicationsByLeaseExpirationTime
    ON WorkItemPublications(LeaseExpirationTime, WorkItemId);

RUN BATCH;
