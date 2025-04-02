# Copyright 2022 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# As a note, default encryption is no long required to be set up as it is on
# by default in S3 as of 2023-01-05.

resource "aws_s3_bucket" "blob_storage" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_acl" "blob_storage" {
  bucket = aws_s3_bucket.blob_storage.id
  acl    = "private"
}

resource "aws_s3_bucket_versioning" "blob_storage" {
  bucket = aws_s3_bucket.blob_storage.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "blob_storage" {
  bucket                  = aws_s3_bucket.blob_storage.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "block_access_from_http" {
  bucket = aws_s3_bucket.blob_storage.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "BlockHttpAccess"
        Action = [
          "s3:*",
        ]
        Effect = "Deny"
        Resource = [
          aws_s3_bucket.blob_storage.arn,
          "${aws_s3_bucket.blob_storage.arn}/*",
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
        "Principal" : "*"
      },
    ]
  })
}

resource "aws_s3_bucket" "blob_storage_logging" {
  bucket = "${var.bucket_name}_logging"
}

resource "aws_s3_bucket_acl" "blob_storage_logging_acl" {
  bucket = "${var.bucket_name}_logging"
  acl    = "log-delivery-write"
}

resource "aws_s3_bucket_logging" "blob_storage" {
  bucket = aws_s3_bucket.blob_storage.id

  target_bucket = aws_s3_bucket.blob_storage_logging.id
  target_prefix = "logs/"
}
