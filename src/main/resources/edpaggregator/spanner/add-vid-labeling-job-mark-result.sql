-- liquibase formatted sql

-- changeset marcopremier:29 dbms:cloudspanner
-- Preserve the original successful Mark result. NULL identifies legacy rows; an empty array
-- records that this job was not responsible for completing any model line.
ALTER TABLE VidLabelingJob ADD COLUMN MarkSucceededCompletedModelLines ARRAY<STRING(MAX)>;
