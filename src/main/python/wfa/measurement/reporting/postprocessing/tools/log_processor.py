# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from absl import app
from absl import flags
from absl import logging
from google.api_core.exceptions import NotFound
from google.cloud import storage
from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

ReportPostProcessorStatus = report_post_processor_result_pb2.ReportPostProcessorStatus
ReportPostProcessorResult = report_post_processor_result_pb2.ReportPostProcessorResult

# Define absl flags for command-line arguments.
FLAGS = flags.FLAGS

flags.DEFINE_string(
    'bucket_name',
    None,
    'The name of the GCS bucket where the report result is stored.',
    required=True
)
flags.DEFINE_string(
    'blob_key',
    None,
    'The full path/key of the blob (file) within the GCS bucket to read.',
    required=True
)
flags.DEFINE_boolean("debug", False, "Enable debug mode.")


def read_and_parse_report_post_processor_result(bucket_name: str,
    blob_key: str) -> ReportPostProcessorResult:
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_key)

  try:
    report_result = ReportPostProcessorResult()
    report_result.ParseFromString(blob.download_as_bytes())
    return report_result
  except NotFound:
    raise ValueError(
        f"Error processing report: The blob key {blob_key} does not exist."
    )
  except Exception as e:
    raise ValueError(f"An error occurred: {e}")


def main():
  # Sends the log to stderr.
  FLAGS.logtostderr = True

  # Sets the log level based on the --debug flag.
  logging.set_verbosity(logging.DEBUG if FLAGS.debug else logging.INFO)

  # Accesses arguments via FLAGS object.
  bucket_name = FLAGS.bucket_name
  blob_key = FLAGS.blob_key

  try:
    result = read_and_parse_report_post_processor_result(bucket_name, blob_key)
    print(result)

    # Exits with success code.
    sys.exit(0)
  except Exception as e:
    # Prints error to stderr.
    print(e, file=sys.stderr)

    # Exits with failure code.
    sys.exit(1)


if __name__ == "__main__":
  app.run(main)
