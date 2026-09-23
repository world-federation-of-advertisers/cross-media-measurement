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

-- Keep the original WorkItemPublications changeset immutable. Earlier development deployments
-- may already have applied it, so retry scheduling is added by this follow-up migration.
ALTER TABLE WorkItemPublications ADD COLUMN NextAttemptTime TIMESTAMP;

ALTER TABLE WorkItemPublications ADD COLUMN QueueResolutionFailed BOOL;

UPDATE WorkItemPublications
SET QueueResolutionFailed = FALSE
WHERE QueueResolutionFailed IS NULL;

ALTER TABLE WorkItemPublications ALTER COLUMN QueueResolutionFailed BOOL NOT NULL;

DROP INDEX WorkItemPublicationsByLeaseExpirationTime;

CREATE INDEX WorkItemPublicationsByClaimPriority
    ON WorkItemPublications(QueueResolutionFailed, NextAttemptTime, LeaseExpirationTime, WorkItemId);
