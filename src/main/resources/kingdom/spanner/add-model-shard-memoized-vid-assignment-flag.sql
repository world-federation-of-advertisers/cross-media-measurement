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

-- changeset marcopremier:35 dbms:cloudspanner
-- comment: Add MemoizedVidAssignmentEnabled column to ModelShards.

START BATCH DDL;

ALTER TABLE ModelShards
  ADD COLUMN MemoizedVidAssignmentEnabled BOOL NOT NULL DEFAULT (FALSE);

RUN BATCH;
