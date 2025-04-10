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
-- comment: Add generated columns for data availability interval start and end times to EventGroups.

ALTER TABLE EventGroups
ADD COLUMN DataAvailabilityStartTime TIMESTAMP AS (
  IF(
    EventGroupDetails.data_availability_interval.start_time IS NULL,
    NULL,
    TIMESTAMP_ADD(
      TIMESTAMP_SECONDS(EventGroupDetails.data_availability_interval.start_time.seconds),
      INTERVAL EventGroupDetails.data_availability_interval.start_time.nanos NANOSECOND
    )
  )
) STORED;

ALTER TABLE EventGroups
ADD COLUMN DataAvailabilityEndTime TIMESTAMP AS (
  IF(
    EventGroupDetails.data_availability_interval.end_time IS NULL,
    NULL,
    TIMESTAMP_ADD(
      TIMESTAMP_SECONDS(EventGroupDetails.data_availability_interval.end_time.seconds),
      INTERVAL EventGroupDetails.data_availability_interval.end_time.nanos NANOSECOND
    )
  )
) STORED;
