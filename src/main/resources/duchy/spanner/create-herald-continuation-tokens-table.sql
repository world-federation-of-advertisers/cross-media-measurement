-- liquibase formatted sql

-- Copyright 2022 The Cross-Media Measurement Authors
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

-- changeset renjiez:2 dbms:cloudspanner

START BATCH DDL;

-- HeraldContinuationTokens
--   A ContinuationToken contains information to locate the progress of
--   Computation streaming for the Herald daemon in a Duchy. The Kingdom uses
--   the content of continuation token as a filter to query Computations and
--   also returns a new continuation token to mark the progress. Duchy stores
--   the latest continuation token in the database every time it receives one.
--   When restart happens, herald retrieve the continuation token to continue
--   streaming.
CREATE TABLE HeraldContinuationTokens (
  Presence BOOL NOT NULL,

  -- The content of the latest ContinuationToken
  ContinuationToken STRING(MAX) NOT NULL,

  -- Last time the ContinuationToken was modified.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  CONSTRAINT presence_set CHECK(Presence),

) PRIMARY KEY (Presence);

RUN BATCH;